WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS pos
    FROM sessions
)
SELECT *
FROM Ranked
WHERE pos <= 2
ORDER BY customer_id DESC;
