SELECT
    id,
    customer_id,
    started_at,
    RANK() OVER (PARTITION BY customer_id ORDER BY started_at DESC) AS started_at_rank
FROM sessions;
