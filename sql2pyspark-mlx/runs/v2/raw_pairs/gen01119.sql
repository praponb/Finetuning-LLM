SELECT
    id,
    amount,
    method,
    CONCAT(method, ', ', status) AS method_status,
    LENGTH(method) AS method_length
FROM payments
