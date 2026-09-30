SELECT order_id, SUBSTRING(method, 1, 2) AS method_prefix, TRIM(status) AS clean_status
FROM payments
WHERE paid_at < '2025-10-01';
