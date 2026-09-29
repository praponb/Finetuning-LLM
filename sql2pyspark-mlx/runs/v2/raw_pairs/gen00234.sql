SELECT customers.is_active, t.first_started_at
FROM customers
JOIN (SELECT customer_id, MIN(started_at) AS first_started_at
    FROM sessions
    GROUP BY customer_id) t ON customers.id = t.customer_id
ORDER BY t.first_started_at ASC;
