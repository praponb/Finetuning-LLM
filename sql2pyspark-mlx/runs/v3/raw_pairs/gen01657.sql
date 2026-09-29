SELECT
    MONTH(opened_date) AS opened_month,
    QUARTER(opened_date) AS opened_quarter,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values,
    SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count
FROM stores
WHERE sqft > 9814
GROUP BY CUBE(MONTH(opened_date), QUARTER(opened_date));
