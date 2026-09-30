SELECT c.city, MAX(a.opened_date) AS last_opened_date, COUNT(DISTINCT a.balance) AS unique_customers
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
GROUP BY c.city
ORDER BY last_opened_date, city
LIMIT 20;
