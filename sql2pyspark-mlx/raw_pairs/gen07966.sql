WITH src AS (
    SELECT *, CONCAT(status, ', ', account_type) AS status_account_type
    FROM accounts
    WHERE account_type IN ('CHECKING', 'CREDIT', 'LOAN')
),
agg AS (
    SELECT MONTH(opened_date) AS opened_month, ROUND(STDDEV(balance), 2) AS stddev_balance, MIN(balance) AS min_balance
    FROM src
    GROUP BY MONTH(opened_date)
)
SELECT opened_month, stddev_balance
FROM agg
ORDER BY opened_month DESC;
