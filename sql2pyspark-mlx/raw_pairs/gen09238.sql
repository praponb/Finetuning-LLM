SELECT YEAR(c.signup_date) AS signup_year, SUM(CASE WHEN a.status = 'OPEN' THEN 1 ELSE 0 END) AS open_count
FROM accounts a
JOIN customers c ON a.customer_id = c.id
GROUP BY YEAR(c.signup_date);
