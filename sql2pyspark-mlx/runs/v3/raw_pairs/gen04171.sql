SELECT c.country, COUNT(s.id) AS subscription_count
FROM customers c
INNER JOIN subscriptions s ON s.customer_id = c.id
GROUP BY c.country
