SELECT DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month, YEAR(c.signup_date) AS signup_year, SUM(CASE WHEN a.account_type = 'CHECKING' THEN a.balance ELSE 0 END) AS checking_balance
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM'), YEAR(c.signup_date)
HAVING SUM(CASE WHEN a.account_type = 'CHECKING' THEN a.balance ELSE 0 END) >= 42
ORDER BY signup_year DESC;
