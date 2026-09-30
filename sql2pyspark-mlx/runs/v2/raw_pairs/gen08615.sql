SELECT
    method,
    paid_at,
    order_id,
    SUBSTRING(status, 1, 3) AS status_prefix,
    SUBSTRING(status, 1, 2) AS status_prefix_2
FROM payments
WHERE status = 'SUCCESS';
