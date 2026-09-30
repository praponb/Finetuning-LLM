SELECT c.segment, SUM(CASE WHEN s.status = 'CANCELLED' THEN s.monthly_fee ELSE 0 END) AS cancelled_monthly_fee
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE UPPER(c.name) LIKE '%OB %'
GROUP BY c.segment
