SELECT
    i.id,
    i.store_id,
    i.product_id,
    s.opened_date
FROM inventory i
LEFT OUTER JOIN stores s ON s.id = i.store_id
WHERE i.quantity <= 216 AND i.store_id IN (1, 3, 5);
