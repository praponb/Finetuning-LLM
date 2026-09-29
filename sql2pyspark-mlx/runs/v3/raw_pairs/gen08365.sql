SELECT c.segment, COUNT(*) subscription_count, SUM(CASE WHEN s.status = 'ACTIVE' THEN 1 ELSE 0 END) active_count
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.credit_limit BETWEEN 9900 AND 13700
GROUP BY c.segment
ORDER BY active_count, segment
LIMIT 10;
