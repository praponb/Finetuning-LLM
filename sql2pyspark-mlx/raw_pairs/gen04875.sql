WITH ranked AS (
    SELECT
        id,
        customer_id,
        started_at,
        country,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY started_at DESC) AS row_num
    FROM sessions
    WHERE country = 'UK'
)
SELECT
    id,
    customer_id,
    started_at,
    country
FROM ranked
WHERE row_num = 1;
