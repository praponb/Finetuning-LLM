WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        status_bits * pressure AS status_bits_times_pressure
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 01:00:00'
      AND firmware IN ('2.0.1', '1.0.3')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
kept_keys AS (
    -- Set difference on keys
    SELECT device_code, firmware
    FROM islands
    EXCEPT DISTINCT
    SELECT device_code, firmware
    FROM device_readings
    WHERE status_bits != 6
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN kept_keys k ON s.device_code = k.device_code AND s.firmware = k.firmware
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY session_no) AS session_no_percent_rank,
        COUNT(temp_c) OVER (PARTITION BY device_code) AS device_code_count_temp_c,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY temp_c NULLS FIRST) AS temp_c_cume_dist
    FROM kept_rows
    WHERE device_code != 'DV-02'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        session_no,
        session_no_percent_rank,
        temp_c_cume_dist,
        COUNT(session_no) OVER w_grp AS session_no_count_2h,
        MAX(device_code_count_temp_c) OVER (PARTITION BY device_code) AS device_code_max_device_code_count_temp_c,
        RANK() OVER (PARTITION BY device_code ORDER BY session_no) AS session_no_rank
    FROM scored
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT device_code, id
    FROM enriched
    EXCEPT ALL
    SELECT device_code, id
    FROM device_readings
    WHERE reading_ts >= '2023-10-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys_2 k ON s.device_code = k.device_code AND s.id = k.id
),
filtered_device_readings AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(device_code)) AS device_code_title,
        temp_c_cume_dist - session_no_percent_rank AS temp_c_cume_dist_minus_session_no_percent_rank
    FROM reconciled
    WHERE session_no_rank = 8
),
stats AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        APPROX_PERCENTILE(temp_c, ARRAY(0.25, 0.5, 0.75)) AS temp_c_quartiles,
        SUM(session_no) FILTER (WHERE device_code = 'DV-04') AS dv_04_total_session_no,
        MIN(reading_ts) AS started_at
    FROM filtered_device_readings
    GROUP BY DATE_TRUNC('HOUR', reading_ts)
    HAVING dv_04_total_session_no > 0
)
SELECT hour_window, temp_c_quartiles
FROM stats
ORDER BY hour_window DESC;
