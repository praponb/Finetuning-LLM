WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        INITCAP(TRIM(country)) AS country_title,
        COALESCE(browser, 'NONE') AS browser_filled
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-08-31 03:00:00'
      AND customer_id IN (28, 31)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT ALL
    SELECT id
    FROM sessions
    WHERE customer_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.id = k.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < '2024-02-09'
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.page NOT IN ('/search', '/home', '/cart')
        )
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(device) AS device_upper
    FROM kept
    WHERE device = 'mobile'
),
not_flagged_sessions AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.duration_sec BETWEEN 153 AND 361
        )
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        device,
        COUNT(DISTINCT device) AS distinct_devices,
        COUNT(CASE WHEN device = 'desktop' THEN 1 END) AS desktop_count,
        COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count
    FROM not_flagged_sessions
    GROUP BY device
)
SELECT device
FROM totals;
