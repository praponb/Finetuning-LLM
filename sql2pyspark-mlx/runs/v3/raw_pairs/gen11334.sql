WITH Numbered AS (
    SELECT
        id,
        browser,
        started_at,
        RANK() OVER (PARTITION BY browser ORDER BY started_at) AS row_num
    FROM sessions
)
SELECT id, browser, started_at
FROM Numbered
WHERE row_num <= 1
ORDER BY id DESC
