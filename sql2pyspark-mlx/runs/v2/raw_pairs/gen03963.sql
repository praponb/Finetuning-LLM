SELECT
    supplier_id,
    brand,
    COUNT(CASE WHEN category = 'Electronics' THEN 1 END) electronics_count,
    MIN(launch_date) min_launch_date,
    COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) umbrella_count
FROM products
GROUP BY supplier_id, brand
ORDER BY umbrella_count, supplier_id, brand
LIMIT 20;
