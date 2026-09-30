SELECT id, customer_id, started_at
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY started_at) AS pos
    FROM sessions
    WHERE customer_id IN (1, 38)
) AS r
WHERE pos <= 5
ORDER BY id DESC
