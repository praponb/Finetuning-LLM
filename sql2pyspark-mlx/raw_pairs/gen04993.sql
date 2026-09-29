SELECT YEAR(customers.signup_date) AS signup_year, MAX(accounts.opened_date) AS latest_opened_date, COUNT(*) AS total_count
FROM accounts
INNER JOIN customers ON accounts.customer_id = customers.id
GROUP BY YEAR(customers.signup_date)
ORDER BY latest_opened_date DESC
