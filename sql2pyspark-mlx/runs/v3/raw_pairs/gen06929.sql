SELECT
    amount,
    method,
    paid_at,
    LOWER(method) AS method_lower
FROM payments
WHERE order_id IN (2, 14)
ORDER BY 3, method_lower
