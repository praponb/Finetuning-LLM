WITH base AS (
    SELECT *
    FROM accounts
    WHERE status IN ('FROZEN', 'OPEN') AND customer_id IS NULL AND status IS NOT NULL
),
AccountsStats AS (
    SELECT MONTH(opened_date) AS opened_month, AVG(balance) AS avg_balance, ROUND((100 * SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_closed
    FROM base
    GROUP BY MONTH(opened_date)
)
SELECT opened_month, avg_balance, pct_closed
FROM AccountsStats
WHERE avg_balance >= 51000
