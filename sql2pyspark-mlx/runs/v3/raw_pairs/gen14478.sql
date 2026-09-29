SELECT
    YEAR(launch_date) AS launch_year,
    CASE WHEN brand IN ('Umbrella', 'Globex') THEN 'tier_1' ELSE 'other' END AS brand_group,
    brand,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values
FROM products
WHERE price BETWEEN 330 AND 1030
GROUP BY 1, 2, 3
ORDER BY brand_group
