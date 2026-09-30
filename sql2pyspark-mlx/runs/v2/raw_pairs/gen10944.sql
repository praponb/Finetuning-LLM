SELECT
    city,
    region,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    ROUND(STDDEV(sqft), 2) AS stddev_sqft,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
FROM stores
WHERE opened_date < '2023-08-01'
GROUP BY city, region;
