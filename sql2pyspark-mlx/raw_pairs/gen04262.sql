SELECT id, browser, started_at
FROM (
    SELECT
        id,
        browser,
        started_at,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) rn
    FROM sessions
) t
WHERE rn = 1
ORDER BY id DESC;
