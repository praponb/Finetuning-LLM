SELECT
    region,
    QUARTER(opened_date) AS opened_quarter,
    YEAR(opened_date) AS opened_year,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values
FROM stores
GROUP BY 1, 2, 3;
