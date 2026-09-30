WITH with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.event_time,
        p2.device,
        p2.browser,
        p2.country
    FROM web_events s
    INNER JOIN sessions p2
        ON s.session_id = p2.id
    WHERE p2.device IN ('mobile', 'tablet', 'desktop')
)
-- Grouped metrics
SELECT
    country,
    session_id,
    MAX(event_time) AS ended_at,
    MIN(event_time) AS first_seen,
    GROUPING(country) AS gid
FROM with_dims
GROUP BY ROLLUP (country, session_id);
