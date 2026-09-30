WITH ranked_accounts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC, id) AS rn
    FROM accounts
    WHERE status = 'FROZEN'
)
SELECT id, status, opened_date
FROM ranked_accounts
WHERE rn <= 2
ORDER BY opened_date DESC, status DESC
