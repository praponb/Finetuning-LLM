WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS rn
    FROM accounts
)
SELECT *
FROM ranked
WHERE rn <= 2
