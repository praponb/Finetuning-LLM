WITH Base AS (
    SELECT *, WEEKOFYEAR(opened_date) AS opened_date_weekofyear
    FROM accounts
    WHERE customer_id IS NULL
),
summary AS (
    SELECT
        account_type,
        QUARTER(opened_date) AS opened_quarter,
        ROUND(AVG(balance), 2) AS avg_balance,
        ROUND((100 * SUM(CASE WHEN account_type = 'SAVINGS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_savings
    FROM Base
    GROUP BY account_type, QUARTER(opened_date)
)
SELECT account_type, avg_balance, pct_savings
FROM summary;
