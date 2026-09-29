SELECT order_id, TRIM(method) AS clean_method
FROM payments
WHERE order_id IS NOT NULL
ORDER BY order_id;
