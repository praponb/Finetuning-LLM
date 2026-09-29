SELECT
    i.id,
    i.last_restock,
    i.quantity,
    i.store_id,
    p.category
FROM inventory i
LEFT JOIN products p ON p.id = i.product_id
ORDER BY quantity DESC
