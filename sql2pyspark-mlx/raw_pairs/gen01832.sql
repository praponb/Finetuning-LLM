SELECT
    c.id,
    c.name,
    c.signup_date,
    t.min_started_at
FROM customers c
JOIN (SELECT customer_id, MIN(started_at) AS min_started_at
    FROM sessions
    GROUP BY customer_id) t ON c.id = t.customer_id
ORDER BY c.id
LIMIT 5;
