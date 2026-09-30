SELECT
    category,
    supplier_id,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count
FROM products
WHERE launch_date > '2024-06-16' OR supplier_id = 4
GROUP BY category, supplier_id
HAVING globex_count < 2;
