WITH numbered AS (
    SELECT
        id,
        country,
        started_at,
        browser,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) pos
    FROM sessions
)
SELECT
    id,
    country,
    started_at,
    browser
FROM numbered
WHERE pos <= 3;
