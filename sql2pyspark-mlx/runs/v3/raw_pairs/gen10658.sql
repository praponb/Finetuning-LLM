SELECT c.segment, COUNT(s.id) AS subscription_count
FROM customers c
JOIN subscriptions s ON c.id = s.customer_id
GROUP BY c.segment
