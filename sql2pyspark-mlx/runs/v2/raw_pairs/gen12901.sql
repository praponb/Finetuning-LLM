SELECT
    i.id,
    i.last_restock,
    i.store_id,
    i.quantity,
    p.launch_date,
    p.brand
FROM inventory i
LEFT JOIN products p ON p.id = i.product_id
WHERE p.price <= 610
