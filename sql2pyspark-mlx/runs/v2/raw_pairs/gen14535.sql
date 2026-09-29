WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS pos
    FROM sessions
    WHERE device <> 'tablet'
)
SELECT *
FROM ranked
WHERE pos = 1
ORDER BY device, customer_id DESC;
