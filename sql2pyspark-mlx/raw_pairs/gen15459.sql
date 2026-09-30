WITH top_sessions AS (
    SELECT
        id,
        browser,
        started_at,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS row_num
    FROM sessions
    WHERE device = 'desktop'
)
SELECT id, browser, started_at
FROM top_sessions
WHERE row_num <= 3
ORDER BY started_at
