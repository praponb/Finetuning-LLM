SELECT c.segment, COUNT(CASE WHEN s.status = 'ACTIVE' THEN 1 END) active_count, MIN(s.monthly_fee) min_monthly_fee
FROM subscriptions s
FULL OUTER JOIN customers c ON s.customer_id = c.id
GROUP BY c.segment
ORDER BY segment;
