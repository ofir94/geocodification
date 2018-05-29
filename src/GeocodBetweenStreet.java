package org.geoserver.openls.geocoding;

import org.geoserver.openls.geocoding.util.Query;
import org.geoserver.openls.geocoding.util.Service;

public class GeocodBetweenStreet {

    private String p_calle;
    private String p_entre1;
    private String p_entre2;
    private String v_select;
    private String v_from;
    private String v_query;
    private String v_orderBy;
    private int v_orderBy_Count;
    private String v_accuracy;


    public GeocodBetweenStreet(String p_calle, String p_entre1, String p_entre2) {
        this.p_calle = p_calle;
        this.p_entre1 = p_entre1;
        this.p_entre2 = p_entre2;

        this.v_select = "";
        this.v_from = "";
        this.v_query = "";
        this.v_orderBy = "";
        this.v_orderBy_Count = 0;
        this.v_accuracy = "";

    }


    public String geocodBetweenStreet( String p_localidad,
                                       String p_municipio,
                                       String p_provincia){

        if(Service.service.equalsIgnoreCase("Oracle")) {

            v_select = "SELECT tipocalle||' ' ||nombre||' entre ' ||tipo_entre1||' ' ||nombre_entre1||' y ' ||tipo_entre2||' ' ||nombre_entre2||GEOCODINGv2.NOT_NULL_OR_EMPTY_LOCALITY(localidad_nombre)||municipio_nombre||', ' ||provincia_nombre as dirgeocod," +
                        "TO_CHAR(sdo_cs.transform(SDO_LRS.CONVERT_TO_STD_GEOM(SDO_LRS.LOCATE_PT(SDO_LRS.CONVERT_TO_LRS_GEOM(geoloc, 3), SDO_GEOM.SDO_LENGTH(geoloc,3)/2)),8307).sdo_point.x)||';'||TO_CHAR(sdo_cs.transform(SDO_LRS.CONVERT_TO_STD_GEOM(SDO_LRS.LOCATE_PT(SDO_LRS.CONVERT_TO_LRS_GEOM(geoloc, 3), SDO_GEOM.SDO_LENGTH(geoloc,3)/2)),8307).sdo_point.y) as coords," +
                        "((score(1)+score(2)+score(3)+score(4)+score(5)+score(6)) / 6) as gc_score";
        }
        else if(Service.service.equalsIgnoreCase("PostGIS")){

            v_select = "SELECT geocod_isnotnull(tipocalle)||' ' ||geocod_isnotnull(nombre)||' entre ' ||geocod_isnotnull(tipo_entre1)||' ' ||geocod_isnotnull(nombre_entre1)||' y ' ||geocod_isnotnull(tipo_entre2)||' ' ||geocod_isnotnull(nombre_entre2)||NOT_NULL_OR_EMPTY_LOCALITY(localidad_nombre)||geocod_isnotnull(municipio_nombre)||', ' ||geocod_isnotnull(provincia_nombre) as dirgeocod," +
                       "Trunc(cast( ST_X(ST_LineInterpolatePoint(geoloc,0.5)) as numeric),6)||';'|| Trunc(cast( ST_Y(ST_LineInterpolatePoint(geoloc,0.5)) as numeric),6) as coords," +
                       "((coalesce(similarity(nombre_calle_norm,'" + p_calle + "'),0)+coalesce(similarity(nombre_entre1_norm,'" + p_entre1 + "'),0)+coalesce(similarity(nombre_entre2_norm,'" + p_entre2 + "'),0)+coalesce(similarity(localidad_nombre_norm,'" + p_localidad + "'),0)+coalesce(similarity(municipio_nombre_norm,'" + p_municipio + "'),0)+coalesce(similarity(provincia_nombre_norm,'" + p_provincia + "'),0)) / 6) as gc_score";
        }

        v_from = " FROM geocod_calles_segmentos ";
        v_orderBy = " ORDER BY gc_score DESC";

        GenerateSubquery generateSubquery = new GenerateSubquery();

        v_query =  generateSubquery.getSubquery(p_calle, "nombre_calle_norm", 1);

        v_from = v_from + " WHERE " + v_query;

        v_orderBy_Count = 1;

        v_accuracy = "S1";

        if(p_entre1 != null && p_entre1.length() > 0) {
            v_query =  generateSubquery.getSubquery(p_entre1, "nombre_entre1_norm", 2);
            v_from = v_from + " AND " + v_query;
            v_orderBy_Count = v_orderBy_Count + 1;
            v_accuracy = v_accuracy + "S1";
        }
        else {
            String replacement = "+score(2)";
            v_select = v_select.replaceAll(replacement, "");
            v_accuracy = v_accuracy + "S0";
        }

        if(p_entre2 != null && p_entre2.length() > 0) {
            v_query =  generateSubquery.getSubquery(p_entre2, "nombre_entre2_norm", 3);
            v_from = v_from + " AND " + v_query;
            v_orderBy_Count = v_orderBy_Count + 1;
            v_accuracy = v_accuracy + "S1E0";
        }
        else {
            String replacement = "+score(3)";
            v_select = v_select.replaceAll(replacement, "");
            v_accuracy = v_accuracy + "S0E0";
        }

        if(p_localidad != null && p_localidad.length() > 0) {
            v_query =  generateSubquery.getSubquery(p_localidad, "localidad_nombre_norm", 4);
            v_from = v_from +  " AND (" + v_query +" OR localidad_nombre_norm is null)";
            v_orderBy_Count = v_orderBy_Count + 1;
            v_accuracy = v_accuracy + "L?";
        }
        else {
            String replacement = "+score(4)";
            v_select = v_select.replaceAll(replacement, "");
            v_accuracy = v_accuracy + "L0";
        }


        if(p_municipio != null && p_municipio.length() > 0) {
            v_query =  generateSubquery.getSubquery(p_municipio, "municipio_nombre_norm", 5);
            v_from = v_from + " AND " + v_query;
            v_orderBy_Count = v_orderBy_Count + 1;
            v_accuracy = v_accuracy + "M1";
        }
        else {
            String replacement = "+score(5)";
            v_select = v_select.replaceAll(replacement, "");
            v_accuracy = v_accuracy + "M0";
        }

        if(p_provincia != null && p_provincia.length() > 0) {
            v_query =  generateSubquery.getSubquery(p_provincia, "provincia_nombre_norm", 6);
            v_from = v_from + " AND " + v_query;
            v_orderBy_Count = v_orderBy_Count + 1;
            v_accuracy = v_accuracy + "P1";
        }
        else {
            String replacement = "+score(6)";
            v_select = v_select.replaceAll(replacement, "");
            v_accuracy = v_accuracy + "P0";
        }


        v_select = v_select.replaceAll( "/ 6", "/ "+ v_orderBy_Count);
        v_select = v_select +", ' " + v_accuracy +" ' as accuracy "+ v_from + v_orderBy;

        return v_select;
    }


    public String geocodBetweenStreetUnknown(String p_unknown0,
                                             String p_unknown1){


        //TODo: Ver not nul or empty locality

        if(Service.service.equalsIgnoreCase("Oracle")) {
            v_select = "SELECT tipocalle||' ' ||nombre||' entre ' ||tipo_entre1||' ' ||nombre_entre1||' y ' ||tipo_entre2||' ' ||nombre_entre2||GEOCODINGv2.NOT_NULL_OR_EMPTY_LOCALITY(localidad_nombre)||municipio_nombre||', ' ||provincia_nombre as dirgeocod," +
                    "TO_CHAR(sdo_cs.transform(SDO_LRS.CONVERT_TO_STD_GEOM(SDO_LRS.LOCATE_PT(SDO_LRS.CONVERT_TO_LRS_GEOM(geoloc, 3), SDO_GEOM.SDO_LENGTH(geoloc,3)/2)),8307).sdo_point.x)||';'||TO_CHAR(sdo_cs.transform(SDO_LRS.CONVERT_TO_STD_GEOM(SDO_LRS.LOCATE_PT(SDO_LRS.CONVERT_TO_LRS_GEOM(geoloc, 3), SDO_GEOM.SDO_LENGTH(geoloc,3)/2)),8307).sdo_point.y) as coords," +
                    "GEOCODINGv2.UNKNOW_SCORE('" + p_unknown1 + "', score(1), score(2), score(3), score(4), score(5), score(6), score(7), score(8), score(9)) as gc_score";

        }
        else if(Service.service.equalsIgnoreCase("PostGIS")){

            v_select = "SELECT geocod_isnotnull(tipocalle)||' ' ||geocod_isnotnull(nombre)||' entre ' ||geocod_isnotnull(tipo_entre1)||' ' ||geocod_isnotnull(nombre_entre1)||' y ' ||geocod_isnotnull(tipo_entre2)||' ' ||geocod_isnotnull(nombre_entre2)||NOT_NULL_OR_EMPTY_LOCALITY(localidad_nombre)||geocod_isnotnull(municipio_nombre)||', ' ||geocod_isnotnull(provincia_nombre) as dirgeocod," +
                    " Trunc(cast( ST_X(ST_LineInterpolatePoint(geoloc,0.5)) as numeric),6)||';'|| Trunc(cast( ST_Y(ST_LineInterpolatePoint(geoloc,0.5)) as numeric),6) as coords, " +
                    "unknown_score('||p_unknown1||'" +
                    ",cast(coalesce(similarity(nombre_calle_norm,'" + p_calle + "'),0) as real)" +
                    ",cast(coalesce(similarity(nombre_entre1_norm,'" + p_entre1 + "'),0) as real)" +
                    ",cast(coalesce(similarity(nombre_entre2_norm,'" + p_entre2 + "'),0) as real)" +
                    ",cast(coalesce(similarity(localidad_nombre_norm,'" + p_unknown1 + "'),0) as real)" +
                    ",cast(coalesce(similarity(municipio_nombre_norm,'" + p_unknown1 + "'),0) as real)" +
                    ",cast(coalesce(similarity(provincia_nombre_norm,'" + p_unknown1 + "'),0) as real)" +
                    ",cast(coalesce(similarity(localidad_nombre_norm,'" + p_unknown0 + "'),0) as real)" +
                    ",cast(coalesce(similarity(municipio_nombre_norm,'" + p_unknown0 + "'),0) as real)" +
                    ",cast(coalesce(similarity(provincia_nombre_norm,'" + p_unknown0 + "'),0) as real)) as gc_score";
        }


        v_from = " FROM geocod_calles_segmentos ";
        v_orderBy = " ORDER BY gc_score DESC";


        GenerateSubquery generateSubquery = new GenerateSubquery();

        v_query = generateSubquery.getSubquery(p_calle, "nombre_calle_norm", 1);

        v_from = v_from + "WHERE " + v_query;

        v_accuracy = "S1";


        if (p_entre1 != null && p_entre1.length() > 0) {
            v_query = generateSubquery.getSubquery(p_entre1, "nombre_entre1_norm", 2);
            v_from = v_from + " AND "+ v_query;
            v_accuracy = v_accuracy + "S1";
        }
        else {
            v_from = v_from.replaceAll(", score(2)", ", NULL");
            v_accuracy = v_accuracy + "S0";
        }

        if (p_entre2 != null && p_entre2.length() > 0) {
            v_query = generateSubquery.getSubquery(p_entre2, "nombre_entre2_norm", 3);
            v_from = v_from + " AND "+ v_query;
            v_accuracy = v_accuracy + "S1E0L?M?P?";
        }
        else {
            v_select.replaceAll(", score(3)", ", NULL");
            v_accuracy = v_accuracy + "S0E0L?M?P?";
        }

        Query query = generateSubquery.getSubqueryUnknown(p_unknown0, p_unknown1, 4,v_select,v_from,v_orderBy);

        v_select = query.getV_select() + ", '" + v_accuracy +"' as accuracy"+ query.getV_from() + query.getV_orderBy();


        return v_select;

    }










}
