SELECT customer_id, MAX(opened_date) AS latest_opened_date
FROM accounts
GROUP BY customer_id
ORDER BY customer_id
LIMIT 10
