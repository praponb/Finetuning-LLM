WITH ranked_sessions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) rnk
    FROM sessions
    WHERE device = 'mobile'
)
SELECT
    id,
    device,
    started_at,
    customer_id
FROM ranked_sessions
WHERE rnk <= 3;
