SELECT
    i.store_id,
    i.product_id,
    i.quantity,
    p.product_name,
    p.launch_date,
    p.brand
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
WHERE i.last_restock > '2023-11-25';
