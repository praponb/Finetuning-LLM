SELECT DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month, COUNT(a.balance) AS balance_count, ROUND((100 * SUM(CASE WHEN a.account_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM');
