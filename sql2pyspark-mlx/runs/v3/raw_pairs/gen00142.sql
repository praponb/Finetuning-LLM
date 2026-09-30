SELECT c.segment, COUNT(s.id) subscription_count, ROUND(AVG(s.monthly_fee), 2) avg_monthly_fee
FROM customers c
LEFT JOIN subscriptions s ON c.id = s.customer_id
GROUP BY c.segment
