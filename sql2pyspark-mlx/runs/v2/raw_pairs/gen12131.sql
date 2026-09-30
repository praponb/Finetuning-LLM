SELECT
    amount,
    paid_at,
    CONCAT_WS('-', status, method) AS status_method_key,
    HOUR(paid_at) AS paid_at_hour
FROM payments;
