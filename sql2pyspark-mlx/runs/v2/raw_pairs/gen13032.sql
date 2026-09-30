WITH ranked AS (
    SELECT
        id,
        browser,
        started_at,
        device,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) pos
    FROM sessions
    WHERE browser IN ('Safari', 'Chrome', 'Edge')
)
SELECT
    id,
    browser,
    started_at,
    device
FROM ranked
WHERE pos <= 5
ORDER BY id DESC, browser;
