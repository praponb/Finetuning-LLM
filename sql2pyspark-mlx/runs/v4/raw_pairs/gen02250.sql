SELECT id, product_id
FROM order_items
WHERE quantity IS NOT NULL
UNION
SELECT id, product_id
FROM order_items
WHERE product_id IS NULL
