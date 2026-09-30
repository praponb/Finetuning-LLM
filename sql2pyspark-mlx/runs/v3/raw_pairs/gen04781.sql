WITH ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY balance DESC) AS row_num
    FROM accounts
)
SELECT *
FROM ranked
WHERE row_num <= 5
