WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE customer_id IS NOT NULL
),
status_summary AS (
    SELECT status, ROUND(STDDEV(balance), 2) stddev_balance, MAX(balance) - MIN(balance) balance_range
    FROM valid_accounts
    GROUP BY status
)
SELECT *
FROM status_summary
WHERE stddev_balance > 40;
