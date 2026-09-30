WITH top_transactions AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS rn
    FROM transactions
)
SELECT *
FROM top_transactions
WHERE rn = 1;
