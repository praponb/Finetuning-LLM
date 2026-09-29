WITH recent_accounts AS (
    SELECT *, TRIM(status) AS clean_status
    FROM accounts
    WHERE opened_date <= '2023-11-01'
),
summary AS (
    SELECT account_type, SUM(balance) AS total_balance
    FROM recent_accounts
    GROUP BY account_type
)
SELECT account_type, total_balance
FROM summary
WHERE total_balance < 146000
ORDER BY total_balance, account_type DESC;
