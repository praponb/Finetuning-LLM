SELECT
    id,
    merchant,
    amount,
    NTILE(4) OVER (PARTITION BY merchant ORDER BY amount, id) AS bucket
FROM transactions
ORDER BY bucket;
