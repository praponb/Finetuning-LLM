SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, COUNT(DISTINCT balance) AS unique_account_types, SUM(CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count
FROM accounts
WHERE status NOT IN ('OPEN', 'CLOSED', 'FROZEN')
GROUP BY opened_month
