SELECT
    MONTH(opened_date) opened_month,
    region,
    SUM(sqft) total_sqft,
    SORT_ARRAY(COLLECT_SET(city)) city_list
FROM stores
WHERE opened_date > '2024-06-01' AND opened_date > '2024-01-01' AND sqft BETWEEN 12396 AND 15004
GROUP BY opened_month, region
HAVING SUM(CASE WHEN city = 'London' THEN sqft ELSE 0 END) >= 35;
