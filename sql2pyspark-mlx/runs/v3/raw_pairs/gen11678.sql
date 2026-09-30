WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS rnk
    FROM sessions
)
SELECT *
FROM Ranked
WHERE rnk <= 3
