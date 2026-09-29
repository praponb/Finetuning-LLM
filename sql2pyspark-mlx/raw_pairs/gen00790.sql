WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), event_time) / 365.25 AS years_since_event_time,
        CASE WHEN event_type IN ('add_to_cart', 'click') THEN 'Priority' ELSE 'Other' END AS event_type_group
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-05-15 06:00:00'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_event_time
    FROM base
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        d.browser,
        d.country
    FROM islands a
    LEFT JOIN sessions d
        ON a.session_id = d.id
),
with_prev_2 AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY browser ORDER BY event_time, id) AS prev_event_time_2
    FROM with_dims
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time_2 IS NULL OR event_time > prev_event_time_2 + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev_2
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_time,
        event_type_group,
        island_id,
        browser,
        SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, session_id
    FROM sessions_web_events
    INTERSECT ALL
    SELECT id, session_id
    FROM web_events
    WHERE event_time < '2023-09-30'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_web_events s
    INNER JOIN kept_keys k ON s.id = k.id AND s.session_id = k.session_id
),
matched_keys AS (
    -- Set difference on keys
    SELECT session_id, id
    FROM reconciled
    EXCEPT DISTINCT
    SELECT session_id, id
    FROM web_events
    WHERE duration_sec > 284
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN matched_keys k ON s.session_id = k.session_id AND s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('YEAR', event_time) AS event_time_period
    FROM kept_rows
    WHERE browser IS NOT NULL
      AND session_id IN (7, 14, 19)
)
-- Aggregate with conditional metrics
SELECT
    session_id,
    CASE WHEN burst_id >= 6 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_tier,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    COUNT(*) AS event_count,
    SUM(island_id) AS total_island_id,
    COUNT(DISTINCT browser) AS distinct_browsers,
    SUM(burst_id) AS total_burst_id
FROM filtered
GROUP BY session_id, burst_id_tier
ORDER BY event_count DESC NULLS FIRST;
