WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS rnk
    FROM sessions
)
SELECT
    id,
    device,
    started_at,
    customer_id
FROM Numbered
WHERE rnk = 1
ORDER BY id;
