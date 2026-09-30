SELECT customer_id, COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) AS expired_count
FROM subscriptions
GROUP BY customer_id
HAVING COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) < 2
ORDER BY customer_id DESC, expired_count DESC;
