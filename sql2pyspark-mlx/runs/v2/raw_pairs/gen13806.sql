SELECT
    id,
    status,
    amount,
    method,
    FIRST_VALUE(method) OVER (PARTITION BY status ORDER BY amount DESC, id) first_method
FROM payments
WHERE amount < 1390
