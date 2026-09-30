SELECT
    i.last_restock,
    i.product_id,
    p.product_name,
    s.city,
    s.country
FROM inventory i
JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON s.id = p.supplier_id
ORDER BY city;
