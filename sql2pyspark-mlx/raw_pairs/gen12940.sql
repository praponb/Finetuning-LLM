SELECT
    id,
    paid_at,
    method,
    CASE WHEN method = 'cash' THEN 1 ELSE 0 END AS is_cash
FROM payments;
