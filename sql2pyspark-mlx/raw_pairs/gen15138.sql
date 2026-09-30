WITH ranked AS (
    SELECT
        id,
        browser,
        started_at
    FROM sessions
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY started_at NULLS FIRST) = 1
),
kept AS (
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < s.started_at
        )
),
eligible AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND HOUR(ch.event_time) BETWEEN 11 AND 19
        )
),
kept_sessions AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.started_at
        )
)
SELECT
    browser,
    COUNT(DISTINCT browser) AS distinct_browsers,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    COUNT(CASE WHEN browser = 'Safari' THEN 1 END) AS safari_count,
    MIN(started_at) AS first_started_at,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    MIN(started_at) AS started_at
FROM kept_sessions
GROUP BY browser
HAVING duration_min < 1000000
ORDER BY browser DESC
