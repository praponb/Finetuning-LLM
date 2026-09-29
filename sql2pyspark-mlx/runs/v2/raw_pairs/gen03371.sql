WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS rnk
    FROM sessions
)
SELECT *
FROM numbered
WHERE rnk = 1
ORDER BY rnk DESC
