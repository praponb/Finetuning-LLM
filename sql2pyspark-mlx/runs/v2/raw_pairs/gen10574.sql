SELECT
    YEAR(launch_date) AS launch_year,
    brand,
    ROUND(AVG(cost), 2) AS avg_cost,
    ROUND(STDDEV(price), 2) AS stddev_price,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values
FROM products
GROUP BY YEAR(launch_date), brand
ORDER BY category_values, launch_year, brand
LIMIT 3
