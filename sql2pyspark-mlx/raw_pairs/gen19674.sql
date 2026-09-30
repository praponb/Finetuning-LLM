SELECT
    id,
    method,
    amount,
    ROUND(amount * 1.15, 2) AS amount_scaled,
    TRIM(status) AS clean_status
FROM payments
ORDER BY clean_status DESC;
