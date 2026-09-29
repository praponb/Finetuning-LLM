SELECT
    p.launch_date,
    i.product_id,
    i.quantity,
    i.store_id
FROM products p
LEFT JOIN inventory i ON i.product_id = p.id
WHERE i.last_restock <= '2025-04-03'
