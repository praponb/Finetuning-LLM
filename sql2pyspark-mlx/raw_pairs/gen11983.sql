SELECT CASE WHEN status IN ('CLOSED', 'OPEN') THEN 'core' ELSE 'other' END AS status_group, MAX(balance) - MIN(balance) AS balance_range
FROM accounts
WHERE account_type IN ('SAVINGS', 'LOAN', 'CREDIT') AND YEAR(opened_date) = 2025
GROUP BY 1
HAVING SUM(balance) > 156000;
