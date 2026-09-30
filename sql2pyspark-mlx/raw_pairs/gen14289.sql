SELECT
    id,
    method,
    amount,
    paid_at,
    HOUR(paid_at) AS paid_at_hour
FROM payments;
