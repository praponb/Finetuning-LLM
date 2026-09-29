WITH ranked_sessions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS row_num
    FROM sessions
    WHERE customer_id IS NOT NULL
)
SELECT *
FROM ranked_sessions
WHERE row_num <= 1
