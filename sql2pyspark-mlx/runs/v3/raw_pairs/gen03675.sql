SELECT id, browser, started_at
FROM (
    SELECT
        id,
        browser,
        started_at,
        RANK() OVER (PARTITION BY browser ORDER BY started_at DESC) rnk
    FROM sessions
) ranked
WHERE rnk <= 1;
