WITH RecentAccounts AS (
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM accounts
    WHERE account_type IN ('SAVINGS', 'CREDIT', 'LOAN') AND opened_date BETWEEN '2024-02-01' AND '2025-03-01'
),
AccountsStats AS (
    SELECT status, ROUND(AVG(balance), 2) AS avg_balance, ROUND(STDDEV(balance), 2) AS stddev_balance
    FROM RecentAccounts
    GROUP BY status
)
SELECT *
FROM AccountsStats
ORDER BY status ASC;
