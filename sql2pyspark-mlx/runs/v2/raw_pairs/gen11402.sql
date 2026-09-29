SELECT QUARTER(c.signup_date) AS signup_quarter, AVG(a.balance) AS avg_balance
FROM accounts a
JOIN customers c ON a.customer_id = c.id
WHERE c.age < 45
GROUP BY QUARTER(c.signup_date)
ORDER BY signup_quarter, avg_balance DESC
