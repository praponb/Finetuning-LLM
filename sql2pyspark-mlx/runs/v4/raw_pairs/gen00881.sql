SELECT DATE_FORMAT(opened_date, 'yyyy-MM') opened_month, account_type, AVG(balance) avg_balance
FROM accounts
WHERE customer_id IN (17, 34, 37)
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM'), account_type
HAVING COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) >= 2;
