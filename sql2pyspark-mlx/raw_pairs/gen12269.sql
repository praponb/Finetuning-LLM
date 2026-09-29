SELECT
    p.launch_date,
    p.price,
    p.category,
    ss.product_id,
    ss.units,
    ss.sale_date
FROM products p
LEFT OUTER JOIN store_sales ss ON ss.product_id = p.id
WHERE ss.store_id IN (2, 4, 5)
ORDER BY launch_date DESC
