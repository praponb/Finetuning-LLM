SELECT
    i.id,
    i.last_restock,
    i.store_id,
    p.launch_date,
    p.product_name,
    s.supplier_name,
    s.city
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY city
