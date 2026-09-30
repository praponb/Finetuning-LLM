SELECT
    region,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft,
    COUNT(*) AS row_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values
FROM stores
GROUP BY region;
