SELECT
    DATE_TRUNC('MONTH', opened_date) opened_month,
    QUARTER(opened_date) opened_quarter,
    COUNT(*) cnt,
    ROUND((100 * SUM(CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_credit,
    MIN(balance) min_balance
FROM accounts
GROUP BY 1, 2
ORDER BY pct_credit, 2 DESC;
