SELECT QUARTER(c.signup_date) AS signup_quarter, SUM(t.amount) AS total_amount
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
WHERE t.account_id IN (5, 18, 27)
GROUP BY QUARTER(c.signup_date);
