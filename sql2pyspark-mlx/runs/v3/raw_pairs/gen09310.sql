SELECT c.country, MIN(a.opened_date) AS min_opened_date FROM accounts a JOIN customers c ON a.customer_id = c.id GROUP BY c.country
