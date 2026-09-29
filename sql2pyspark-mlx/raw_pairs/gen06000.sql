WITH filtered AS (
    SELECT
        *,
        status_bits + temp_c AS status_bits_plus_temp_c,
        temp_c + status_bits AS temp_c_plus_status_bits
    FROM device_readings
    WHERE pressure BETWEEN 993 AND 995
),
with_windows AS (
    SELECT
        *,
        LAG(temp_c) OVER (PARTITION BY device_code ORDER BY pressure, id) AS prev_temp_c,
        LAG(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id) AS prev_humidity,
        LEAD(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id) AS next_humidity,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure, id) AS event_seq,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS status_bits_bucket
    FROM filtered
),
metrics AS (
    SELECT
        *,
        AVG(prev_temp_c) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_prev_temp_c,
        MAX(prev_temp_c) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_prev_temp_c,
        COUNT(prev_temp_c) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_prev_temp_c,
        MIN(prev_temp_c) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_prev_temp_c
    FROM with_windows
),
flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM metrics
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
stats AS (
    SELECT
        CASE WHEN status_bits_plus_temp_c >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_bits_plus_temp_c_tier,
        DATE_TRUNC('DAY', reading_ts) AS activity_day,
        SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
        COUNT_IF(device_code = 'DV-01') AS dv_01_events,
        MIN(reading_ts) AS started_at,
        ROUND(AVG(island_id), 2) AS avg_island_id,
        BIT_OR(status_bits) AS status_bits_bit_or
    FROM islands
    GROUP BY 1, 2
)
SELECT
    status_bits_plus_temp_c_tier,
    activity_day,
    device_code_list,
    dv_01_events,
    started_at,
    status_bits_bit_or
FROM stats
ORDER BY activity_day DESC NULLS LAST;
