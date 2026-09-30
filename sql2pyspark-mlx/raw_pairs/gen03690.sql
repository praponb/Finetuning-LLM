WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(country, 'unassigned') AS country_filled,
        COALESCE(country, 'NONE') AS country_clean
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2023-09-05 09:00:00')
      AND HOUR(started_at) BETWEEN 10 AND 13
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser, customer_id ORDER BY started_at NULLS FIRST, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM deduped
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        SUM(is_new_session) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, browser
    FROM islands
    EXCEPT
    SELECT customer_id, browser
    FROM sessions
    WHERE country IN ('DE', 'JP', 'SG')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.browser = k.browser
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND TO_DATE(c.event_time) < '2025-08-01'
        )
)
-- Multi-level aggregation
SELECT
    browser,
    burst_id,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    MAX(burst_id) FILTER (WHERE device = 'tablet') AS tablet_max_burst_id
FROM not_flagged
GROUP BY CUBE (browser, burst_id)
ORDER BY tablet_max_burst_id DESC
