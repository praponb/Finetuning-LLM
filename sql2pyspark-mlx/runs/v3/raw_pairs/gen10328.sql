SELECT
    id,
    method,
    status,
    amount,
    CAST(amount AS INT) AS amount_int
FROM payments;
