SELECT id, order_id, product_id
FROM order_items
WHERE NOT (product_id IN (23, 37, 40) AND product_id IS NULL)
