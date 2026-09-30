WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE MONTH(opened_date) IN (5, 10, 11) AND (account_type = 'SAVINGS' OR status IS NOT NULL)
),
agg AS (
    SELECT status, SUM(CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END) AS frozen_count
    FROM valid_accounts
    GROUP BY status
)
SELECT status, frozen_count
FROM agg
WHERE frozen_count < 25
ORDER BY status DESC
LIMIT 20
