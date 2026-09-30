WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS row_num
    FROM sessions
)
SELECT id, device, started_at
FROM numbered
WHERE row_num <= 3
ORDER BY 3, device
