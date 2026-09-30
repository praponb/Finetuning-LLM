SELECT
    c.id,
    c.city,
    c.segment,
    t.closed_count,
    t.avg_balance
FROM customers c
INNER JOIN (SELECT customer_id, SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) closed_count, AVG(balance) avg_balance
    FROM accounts
    WHERE status IS NULL
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.closed_count >= 53
ORDER BY c.city, c.segment
