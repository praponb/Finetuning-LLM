SELECT
    id,
    browser,
    started_at,
    ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS rn
FROM sessions;
