SELECT c.city, t.min_start_date, t.subscription_count
FROM customers c
INNER JOIN (SELECT customer_id, MIN(start_date) AS min_start_date, COUNT(*) AS subscription_count
    FROM subscriptions
    GROUP BY customer_id) t ON c.id = t.customer_id
ORDER BY c.city
