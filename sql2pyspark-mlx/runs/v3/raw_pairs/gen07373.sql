SELECT
    i.id,
    i.product_id,
    ss.store_id,
    ss.units
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.quantity BETWEEN 79 AND 211
ORDER BY id
