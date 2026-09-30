SELECT
    id,
    browser,
    started_at,
    country
FROM (
    SELECT
        id,
        browser,
        started_at,
        country,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS rn
    FROM sessions
    WHERE device = 'desktop'
) AS t
WHERE rn <= 1
