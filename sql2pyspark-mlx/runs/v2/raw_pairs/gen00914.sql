SELECT
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    customer_id,
    SUM(balance) AS total_balance,
    ROUND((100 * SUM(CASE WHEN account_type = 'SAVINGS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_savings
FROM accounts
WHERE status IN ('OPEN', 'FROZEN')
GROUP BY DATE_TRUNC('MONTH', opened_date), customer_id
HAVING total_balance > 198000
