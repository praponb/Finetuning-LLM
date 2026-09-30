SELECT
    i.id,
    i.product_id,
    i.quantity,
    s.store_name
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
ORDER BY id
