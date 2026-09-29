SELECT
    i.id,
    i.product_id,
    p.brand,
    p.product_name
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
WHERE i.quantity BETWEEN 147 AND 149 AND i.product_id IN (3, 5, 9)
ORDER BY id DESC
