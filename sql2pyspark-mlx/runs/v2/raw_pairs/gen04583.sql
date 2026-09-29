SELECT
    id,
    browser,
    started_at,
    RANK() OVER (PARTITION BY browser ORDER BY started_at) AS started_at_rank
FROM sessions
WHERE country = 'TH'
ORDER BY browser;
