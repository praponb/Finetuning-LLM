WITH top_accounts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS pos
    FROM accounts
)
SELECT
    id,
    status,
    balance,
    opened_date
FROM top_accounts
WHERE pos <= 3
ORDER BY balance DESC;
