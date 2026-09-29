SELECT
    id,
    paid_at,
    amount,
    method,
    LOWER(status) AS status_lower
FROM payments
WHERE order_id IN (3, 11, 23);
