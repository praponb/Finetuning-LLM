SELECT
    city,
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values,
    ROUND((100 * SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_south,
    SUM(CASE WHEN city = 'New York' THEN sqft ELSE 0 END) AS new_york_sqft
FROM stores
GROUP BY city, DATE_TRUNC('MONTH', opened_date)
ORDER BY 2
