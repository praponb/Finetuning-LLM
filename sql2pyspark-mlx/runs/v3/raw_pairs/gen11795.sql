SELECT
    id,
    paid_at,
    method,
    amount,
    LENGTH(method) AS method_length
FROM payments
WHERE method = 'paypal'
