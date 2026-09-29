SELECT
    p.brand,
    i.product_id,
    i.store_id,
    i.last_restock
FROM products p
LEFT JOIN inventory i ON i.product_id = p.id
WHERE i.store_id IN (1, 2, 6)
ORDER BY i.product_id
