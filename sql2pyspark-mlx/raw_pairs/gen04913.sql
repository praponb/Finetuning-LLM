SELECT
    i.id,
    i.product_id,
    s.store_name,
    p.brand
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
LEFT JOIN products p ON i.product_id = p.id
WHERE i.store_id IN (1, 4, 6)
ORDER BY store_name DESC, id
LIMIT 5
