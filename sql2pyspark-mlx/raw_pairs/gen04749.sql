WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS latest_rank
    FROM sessions
    QUALIFY latest_rank = 1
)
SELECT
    device,
    browser,
    MAX(started_at) AS last_seen,
    MIN(started_at) AS first_seen,
    MAX(started_at) AS last_seen_2,
    MIN(started_at) AS first_seen_2,
    COUNT(*) AS event_count,
    MAX(started_at) AS last_started_at
FROM latest
GROUP BY CUBE (device, browser)
ORDER BY browser
