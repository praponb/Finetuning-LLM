WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        DATE_FORMAT(reading_ts, 'MMM yyyy') AS reading_ts_label
    FROM device_readings
    WHERE humidity IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_status_bits,
        MIN(status_bits) OVER (PARTITION BY device_code ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_status_bits,
        COUNT(status_bits) OVER (PARTITION BY device_code ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_status_bits,
        MAX(status_bits) OVER (PARTITION BY device_code ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_status_bits,
        NTILE(5) OVER (ORDER BY status_bits, id) AS status_bits_bucket
    FROM islands
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        pressure,
        status_bits,
        reading_ts_label,
        is_new_burst,
        session_id,
        running_avg_status_bits,
        running_max_status_bits,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY running_count_status_bits DESC NULLS FIRST) AS running_count_status_bits_dense_rank,
        LAG(running_min_status_bits, 1) OVER (ORDER BY reading_ts, id) AS prev_running_min_status_bits
    FROM metrics
    WHERE device_code IN ('DV-02', 'DV-04')
)
-- Multi-level aggregation
SELECT
    CASE WHEN pressure >= 1030 THEN 'PREMIUM' ELSE 'STANDARD' END AS pressure_tier,
    session_id,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count,
    COUNT(*) AS cnt,
    GROUPING(CASE WHEN pressure >= 1030 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM metrics_device_readings
GROUP BY ROLLUP (CASE WHEN pressure >= 1030 THEN 'PREMIUM' ELSE 'STANDARD' END, session_id)
ORDER BY dv_04_count DESC
