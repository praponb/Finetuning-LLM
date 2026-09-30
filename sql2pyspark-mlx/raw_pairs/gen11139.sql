SELECT DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month, MAX(a.balance) AS max_balance
FROM accounts a
JOIN customers c ON c.id = a.customer_id
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM')
ORDER BY max_balance DESC;
