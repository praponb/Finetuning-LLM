WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        SUM(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        NTILE(4) OVER (PARTITION BY YEAR(event_time), page ORDER BY duration_sec, id) AS duration_sec_bucket
    FROM web_events
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY event_type, page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY event_type, page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY event_type, page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_duration_sec
    FROM enriched
    WHERE duration_sec = 147
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        event_type,
        event_time,
        duration_sec_bucket
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY running_sum_duration_sec, id) = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM ranked
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_type,
        event_time,
        duration_sec_bucket,
        SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
islands_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps_2
)
-- Multi-level aggregation
SELECT
    CASE WHEN island_id >= 9 THEN 'HIGH' ELSE 'LOW' END AS island_id_tier,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    COUNT_IF(event_type = 'click') AS click_events,
    MAX(event_time) AS last_event_time,
    PERCENTILE_APPROX(island_id, ARRAY(0.25, 0.5, 0.75)) AS island_id_quartiles,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN island_id >= 9 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT_IF(event_type = 'click') DESC) AS click_events_percent_rank
FROM islands_web_events
GROUP BY CASE WHEN island_id >= 9 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', event_time);
