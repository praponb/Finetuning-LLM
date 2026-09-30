WITH ranked_sessions AS (
    SELECT
        id,
        browser,
        started_at,
        device,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) rn
    FROM sessions
    WHERE browser IN ('Safari', 'Edge')
)
SELECT
    id,
    browser,
    started_at,
    device
FROM ranked_sessions
WHERE rn <= 3
