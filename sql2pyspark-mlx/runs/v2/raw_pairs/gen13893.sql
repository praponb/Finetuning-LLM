SELECT c.id, c.country, COUNT(s.id) AS subscription_count FROM customers c LEFT OUTER JOIN subscriptions s ON s.customer_id = c.id GROUP BY c.id, c.country ORDER BY subscription_count DESC, c.country
