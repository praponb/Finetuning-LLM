SELECT
    i.id,
    i.store_id,
    i.last_restock,
    i.product_id,
    p.brand,
    p.product_name
FROM inventory i
JOIN products p ON i.product_id = p.id
WHERE YEAR(i.last_restock) >= 2024
