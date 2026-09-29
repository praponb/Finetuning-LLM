SELECT
    id,
    device,
    started_at,
    customer_id
FROM (
    SELECT
        id,
        device,
        started_at,
        customer_id,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY started_at DESC) AS rnk
    FROM sessions
) AS t
WHERE rnk <= 3;
