WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM sessions s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_type <> 'add_to_cart'
        )
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.browser,
        c.segment
    FROM eligible e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < '2024-12-01'
        )
),
eligible_sessions AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_type IN ('add_to_cart', 'search')
        )
)
-- Grouped metrics
SELECT
    browser,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    COUNT(DISTINCT browser) AS distinct_browsers
FROM eligible_sessions
GROUP BY browser
HAVING SIZE(COLLECT_SET(segment)) < 1000
ORDER BY num_segments DESC
