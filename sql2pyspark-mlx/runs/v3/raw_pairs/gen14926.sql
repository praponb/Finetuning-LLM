SELECT
    p.id,
    p.stock_qty,
    p.product_name,
    p.category,
    s.rating,
    s.city
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.brand != 'Initech' AND p.launch_date > '2025-04-01'
ORDER BY s.city ASC
