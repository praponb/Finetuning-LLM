SELECT
    id,
    country,
    started_at,
    browser
FROM (
    SELECT
        id,
        country,
        started_at,
        browser,
        RANK() OVER (PARTITION BY country ORDER BY started_at DESC) AS rn
    FROM sessions
) AS ranked
WHERE rn <= 2
