SELECT
    id,
    started_at,
    browser,
    RANK() OVER (PARTITION BY YEAR(started_at), browser ORDER BY started_at) AS rnk
FROM sessions
WHERE HOUR(started_at) BETWEEN 11 AND 18
