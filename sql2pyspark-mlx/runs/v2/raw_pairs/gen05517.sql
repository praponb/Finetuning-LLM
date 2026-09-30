SELECT
    p.launch_date,
    p.product_name,
    p.cost,
    ss.product_id,
    ss.units,
    ss.sale_date,
    COALESCE(p.product_name, 'Unknown') AS product_name_filled
FROM products p
LEFT JOIN store_sales ss ON ss.product_id = p.id
WHERE ss.store_id IN (1, 2, 3, 5)
ORDER BY ss.units DESC;
