SELECT order_id, status, SUBSTRING(method, 1, 3) AS method_prefix
FROM payments
WHERE NOT (order_id IS NULL AND amount < 1470 AND order_id IS NULL);
