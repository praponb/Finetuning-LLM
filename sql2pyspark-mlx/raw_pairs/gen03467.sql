WITH base AS (
    SELECT
        *,
        HOUR(reading_ts) AS reading_ts_hour,
        CASE WHEN device_code IN ('DV-03', 'DV-01') THEN 'Priority' ELSE 'Other' END AS device_code_group
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND status_bits >= 5
),
flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows AS (
    SELECT
        *,
        LAG(burst_id) OVER (PARTITION BY device_code ORDER BY burst_id, id) AS prev_burst_id,
        LAG(status_bits) OVER (PARTITION BY device_code ORDER BY burst_id, id) AS prev_status_bits,
        LEAD(status_bits) OVER (PARTITION BY device_code ORDER BY burst_id, id) AS next_status_bits,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY burst_id, id) AS event_seq
    FROM sessions_device_readings
),
valid AS (
    SELECT
        id,
        device_code,
        reading_ts,
        burst_id,
        prev_burst_id,
        event_seq,
        LENGTH(device_code) AS device_code_length
    FROM with_windows
    WHERE reading_ts >= '2024-11-08'
),
flagged_2 AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
islands AS (
    SELECT
        id,
        device_code,
        reading_ts,
        event_seq,
        device_code_length,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged_2
),
daily AS (
    SELECT
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        DATE_TRUNC('DAY', reading_ts) AS activity_day,
        COUNT(DISTINCT id) AS distinct_ids,
        APPROX_PERCENTILE(device_code_length, 0.95) AS device_code_length_p95,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', reading_ts) ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_dense_rank
    FROM islands
    GROUP BY 1, 2
)
SELECT
    hour_window,
    activity_day,
    distinct_ids,
    device_code_length_p95,
    distinct_ids_dense_rank
FROM daily
