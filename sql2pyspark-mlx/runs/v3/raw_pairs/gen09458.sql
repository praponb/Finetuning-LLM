SELECT
    id,
    customer_id,
    started_at,
    country
FROM (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY started_at DESC) AS pos
    FROM sessions
) AS r
WHERE pos <= 3
