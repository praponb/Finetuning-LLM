WITH top_accounts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS rn
    FROM accounts
    WHERE status NOT IN ('CLOSED', 'FROZEN')
)
SELECT *
FROM top_accounts
WHERE rn <= 3
ORDER BY status, account_type;
