SELECT
    i.id,
    i.last_restock,
    i.quantity,
    p.launch_date,
    p.brand,
    s.city,
    s.country
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
