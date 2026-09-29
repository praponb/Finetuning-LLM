SELECT c.id, COUNT(s.id) AS subscription_count
FROM customers c
LEFT JOIN subscriptions s ON c.id = s.customer_id
WHERE MONTH(c.signup_date) IN (1, 4, 5)
GROUP BY c.id
HAVING COUNT(s.id) > 5
ORDER BY subscription_count DESC, id DESC
LIMIT 50;
