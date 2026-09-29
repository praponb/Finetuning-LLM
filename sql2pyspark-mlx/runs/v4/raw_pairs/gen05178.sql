SELECT
    id,
    method,
    LOWER(status) AS status_lower,
    LOWER(method) AS method_lower
FROM payments
WHERE amount BETWEEN 1410 AND 1570 AND order_id = 19 AND status IN ('FAILED', 'REFUNDED')
