SELECT
    s.country,
    s.rating,
    s.city,
    p.price,
    p.category,
    p.stock_qty
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
WHERE p.launch_date BETWEEN '2024-05-01' AND '2025-01-03'
ORDER BY category
