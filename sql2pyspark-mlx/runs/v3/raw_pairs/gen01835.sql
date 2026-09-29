SELECT
    id,
    merchant,
    amount,
    RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS rnk
FROM transactions
