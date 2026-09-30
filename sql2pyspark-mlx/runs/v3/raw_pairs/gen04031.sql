SELECT id, discount
FROM orders
WHERE discount > (SELECT MAX(discount) FROM orders WHERE store_id IS NULL)
