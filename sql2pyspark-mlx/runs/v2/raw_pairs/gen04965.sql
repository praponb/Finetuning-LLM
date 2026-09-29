WITH top_sessions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS row_num
    FROM sessions
)
SELECT *
FROM top_sessions
WHERE row_num <= 3
