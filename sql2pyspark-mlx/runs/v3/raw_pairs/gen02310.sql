SELECT
    i.id,
    i.last_restock,
    i.quantity,
    p.product_name,
    s.country,
    s.supplier_name
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON s.id = p.supplier_id
WHERE p.category = 'Sports' AND p.launch_date BETWEEN '2023-06-01' AND '2024-05-01'
ORDER BY country, i.last_restock, id
LIMIT 5
