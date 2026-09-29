WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        device,
        browser,
        started_at,
        session_no
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY session_no, id) AS rn
        FROM islands
    ) x
    WHERE rn = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < s.started_at
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
              AND ch.event_time >= s.started_at
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at,
        INITCAP(TRIM(device)) AS device_title
    FROM not_flagged
    WHERE started_at >= TO_TIMESTAMP('2023-08-30 12:00:00')
      AND browser IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        browser,
        started_at,
        session_no,
        device_title,
        days_since_started_at - session_no AS days_since_started_at_minus_session_no
    FROM cleaned
    WHERE browser IS NOT NULL
      AND days_since_started_at BETWEEN 21 AND 35
)
SELECT
    id,
    device,
    browser,
    started_at,
    session_no,
    device_title
FROM filtered;
