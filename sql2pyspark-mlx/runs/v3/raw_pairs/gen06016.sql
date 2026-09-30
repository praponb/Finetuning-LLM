SELECT
    i.product_id,
    i.last_restock,
    i.store_id,
    p.cost,
    p.launch_date
FROM inventory i
JOIN products p ON i.product_id = p.id
