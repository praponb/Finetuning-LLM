WITH numbered AS (
    SELECT
        id,
        merchant,
        amount,
        DENSE_RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS rnk
    FROM transactions
)
SELECT id, merchant, amount
FROM numbered
WHERE rnk <= 1
ORDER BY id DESC
