SELECT region, city_list, avg_sqft
FROM (
    SELECT region, SORT_ARRAY(COLLECT_SET(city)) city_list, AVG(sqft) avg_sqft
    FROM stores
    GROUP BY region
) x
WHERE avg_sqft > 5100
ORDER BY avg_sqft DESC;
