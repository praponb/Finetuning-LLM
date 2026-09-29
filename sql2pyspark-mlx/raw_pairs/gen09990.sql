SELECT
    DATE_FORMAT(opened_date, 'yyyy-MM') opened_month,
    city,
    ROUND(STDDEV(sqft), 2) stddev_sqft,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) city_values
FROM stores
WHERE sqft IS NOT NULL
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM'), city;
