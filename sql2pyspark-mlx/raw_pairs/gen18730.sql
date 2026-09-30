WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        CEIL(status_bits) AS status_bits_whole
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 07:00:00'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM cleaned
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        firmware,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions_device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        firmware,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps_2
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        reading_ts,
        SUM(burst_id) OVER w_running AS running_sum_burst_id,
        COUNT(burst_id) OVER w_running AS running_count_burst_id,
        MIN(burst_id) OVER w_running AS running_min_burst_id,
        MAX(burst_id) OVER w_running AS running_max_burst_id
    FROM islands
    WINDOW w_running AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        device_code,
        reading_ts,
        running_min_burst_id,
        running_max_burst_id
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY running_max_burst_id DESC, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(running_min_burst_id) OVER (PARTITION BY device_code ORDER BY running_min_burst_id, id) AS prev_running_min_burst_id,
        LEAD(running_min_burst_id) OVER (PARTITION BY device_code ORDER BY running_min_burst_id, id) AS next_running_min_burst_id,
        LAG(running_max_burst_id) OVER (PARTITION BY device_code ORDER BY running_min_burst_id, id) AS prev_running_max_burst_id,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY running_min_burst_id, id) AS seq_no,
        SUM(running_max_burst_id) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_max_burst_id
    FROM ranked
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('DAY', reading_ts) AS activity_day,
        (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
        COUNT(*) AS event_count,
        GROUPING(DATE_TRUNC('DAY', reading_ts)) AS gid
    FROM scored
    GROUP BY ROLLUP (DATE_TRUNC('DAY', reading_ts))
)
SELECT activity_day, event_count
FROM totals
ORDER BY activity_day DESC, event_count;
