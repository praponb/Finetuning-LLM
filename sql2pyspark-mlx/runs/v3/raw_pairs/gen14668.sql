SELECT status, ROUND((100 * SUM(CASE WHEN account_type = 'LOAN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_loan
FROM accounts
WHERE MONTH(opened_date) IN (5, 7, 9)
GROUP BY status
