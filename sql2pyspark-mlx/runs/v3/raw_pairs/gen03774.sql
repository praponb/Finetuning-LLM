SELECT quantity, order_id
FROM order_items
WHERE product_id IS NOT NULL
UNION
SELECT quantity, order_id
FROM order_items
WHERE quantity <> 7
ORDER BY order_id
