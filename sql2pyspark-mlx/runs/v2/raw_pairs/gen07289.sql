SELECT
    i.id,
    i.store_id,
    i.product_id,
    i.quantity,
    s.store_name
FROM inventory i
JOIN stores s ON s.id = i.store_id
WHERE s.city = 'Bangkok';
