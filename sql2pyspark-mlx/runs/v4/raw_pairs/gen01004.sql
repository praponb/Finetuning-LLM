WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        browser,
        started_at,
        country,
        CASE WHEN device IN ('mobile', 'desktop') THEN 'Tier 1' ELSE 'Other' END AS device_group
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-06-10 22:00:00'
      AND country IS NOT NULL
      AND customer_id IN (7, 19, 32)
),
kept_keys AS (
    -- Set difference on keys
    SELECT browser, id
    FROM filtered
    EXCEPT DISTINCT
    SELECT browser, id
    FROM sessions
    WHERE browser = 'Edge'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.browser = k.browser AND s.id = k.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.started_at
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time <= s.started_at
        )
),
not_flagged_sessions AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time <= s.started_at
        )
),
not_flagged_sessions_2 AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', started_at) AS quarter_start,
        COUNT_IF(country = 'JP') AS jp_events,
        MAX(started_at) AS last_started_at,
        COUNT(*) FILTER (WHERE browser = 'Safari') AS safari_rows
    FROM not_flagged_sessions_2
    GROUP BY DATE_TRUNC('QUARTER', started_at)
)
SELECT quarter_start, safari_rows
FROM summary
ORDER BY quarter_start DESC
