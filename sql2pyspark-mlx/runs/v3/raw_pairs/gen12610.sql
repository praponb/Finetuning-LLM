SELECT
    ss.id,
    ss.units,
    ss.store_id,
    p.launch_date,
    p.category,
    s.rating,
    s.country
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
RIGHT JOIN suppliers s ON s.id = p.supplier_id
WHERE ss.store_id = 5
ORDER BY ss.id, store_id DESC;
