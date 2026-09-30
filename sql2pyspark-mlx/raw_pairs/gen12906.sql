WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(temp_c) temp_c_whole
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND status_bits > 12
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        status_bits,
        temp_c_whole,
        is_new_session,
        COUNT(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) status_bits_count_30m,
        AVG(island_id) OVER (ORDER BY pressure, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) island_id_avg_6_prior,
        ROW_NUMBER() OVER (ORDER BY is_new_session, id) event_seq
    FROM sessionized
    WHERE firmware <> '2.0.1'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT device_code, id
    FROM metrics
    INTERSECT
    SELECT device_code, id
    FROM device_readings
    WHERE pressure > 1013
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.device_code = k.device_code AND s.id = k.id
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        pressure,
        AVG(status_bits) OVER (PARTITION BY device_code) device_code_avg_status_bits,
        MAX(status_bits) OVER (PARTITION BY device_code) device_code_max_status_bits
    FROM reconciled
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(device_code_avg_status_bits) OVER (PARTITION BY device_code) device_code_count_device_code_avg_status_bits,
        MAX(device_code_max_status_bits) OVER (PARTITION BY device_code) device_code_max_device_code_max_status_bits
    FROM metrics_device_readings
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN pressure >= 1033 THEN 'LARGE' ELSE 'SMALL' END AS pressure_tier,
    CASE WHEN device_code_count_device_code_avg_status_bits >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END AS device_code_count_device_code_avg_status_bits_tier,
    ROUND(SUM(device_code_max_status_bits) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) AS device_code_max_status_bits_per_dv_01,
    ROUND(AVG(device_code_count_device_code_avg_status_bits), 2) AS avg_device_code_count_device_code_avg_status_bits,
    COUNT(*) AS row_count
FROM with_windows
GROUP BY CUBE (CASE WHEN pressure >= 1033 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN device_code_count_device_code_avg_status_bits >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END);
