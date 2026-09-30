WITH ranked AS (
    SELECT
        id,
        merchant,
        amount,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS rnk
    FROM transactions
)
SELECT id, merchant, amount
FROM ranked
WHERE rnk <= 3
ORDER BY 3, merchant
