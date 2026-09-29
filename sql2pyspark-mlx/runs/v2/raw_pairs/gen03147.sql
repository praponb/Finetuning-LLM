WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE status NOT IN ('CLOSED', 'OPEN', 'FROZEN') AND balance BETWEEN 34000 AND 63000
),
account_type_summary AS (
    SELECT account_type, status, ROUND(STDDEV(balance), 2) AS stddev_balance
    FROM valid_accounts
    GROUP BY account_type, status
)
SELECT account_type, status, stddev_balance
FROM account_type_summary
WHERE stddev_balance < 79
ORDER BY status;
