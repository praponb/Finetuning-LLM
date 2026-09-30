SELECT c.segment, MAX(s.monthly_fee) AS max_monthly_fee
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY c.segment
ORDER BY segment DESC
