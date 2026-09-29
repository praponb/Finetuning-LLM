SELECT
    CASE WHEN balance >= 72000 THEN 'high' WHEN balance >= 11000 THEN 'mid' ELSE 'low' END balance_bucket,
    MIN(opened_date) first_opened_date,
    COUNT(CASE WHEN status = 'OPEN' THEN 1 END) open_count,
    ROUND((100 * SUM(CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_credit
FROM accounts
GROUP BY CASE WHEN balance >= 72000 THEN 'high' WHEN balance >= 11000 THEN 'mid' ELSE 'low' END
ORDER BY open_count DESC
