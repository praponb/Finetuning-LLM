SELECT *
FROM (
    SELECT customer_id, status, SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS web_count
    FROM orders
    GROUP BY customer_id, status
) AS s
WHERE web_count >= 41;
