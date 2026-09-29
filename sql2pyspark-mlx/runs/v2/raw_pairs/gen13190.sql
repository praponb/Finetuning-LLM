SELECT customer_id, status, cancelled_count
FROM (
    SELECT customer_id, status, COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_count
    FROM subscriptions
    GROUP BY customer_id, status
) AS t
WHERE cancelled_count < 9
ORDER BY cancelled_count DESC, status
