WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS rn
    FROM sessions
)
SELECT
    id,
    browser,
    started_at,
    device
FROM numbered
WHERE rn = 1
ORDER BY started_at DESC;
