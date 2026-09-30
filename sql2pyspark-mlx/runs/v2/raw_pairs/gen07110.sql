WITH ranked AS (
    SELECT
        id,
        account_type,
        balance,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS rnk
    FROM accounts
)
SELECT *
FROM ranked
WHERE rnk <= 3
ORDER BY account_type DESC;
