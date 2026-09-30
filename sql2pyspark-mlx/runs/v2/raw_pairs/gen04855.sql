WITH filtered_accounts AS (
    SELECT *
    FROM accounts
    WHERE balance BETWEEN 24000 AND 32000
),
status_summary AS (
    SELECT status, SUM(balance) AS total_balance, COUNT(*) AS row_count
    FROM filtered_accounts
    GROUP BY status
)
SELECT status, row_count
FROM status_summary
WHERE row_count > 6
ORDER BY 2;
