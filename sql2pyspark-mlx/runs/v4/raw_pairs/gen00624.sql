WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_FORMAT(reading_ts, 'yyyy-MM') AS reading_ts_label
    FROM device_readings
    WHERE humidity <= 54
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) AS prev_reading_ts
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY new_group_flag ASC NULLS FIRST, id ASC) AS rn
    FROM sessions_device_readings
    QUALIFY rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        reading_ts_label,
        prev_reading_ts,
        new_group_flag,
        island_id,
        rn,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY island_id ASC, id ASC) AS row_seq,
        AVG(humidity) OVER (PARTITION BY device_code) AS device_code_avg_humidity,
        ROW_NUMBER() OVER (ORDER BY reading_ts ASC, id ASC) AS row_seq_2,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share,
        AVG(island_id) OVER (ORDER BY CAST(prev_reading_ts AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS island_id_avg_90d
    FROM ranked
)
-- Multi-level aggregation
SELECT
    device_code,
    island_id,
    CASE WHEN row_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_2_tier,
    APPROX_PERCENTILE(row_seq_2, ARRAY(0.25, 0.5, 0.75)) AS row_seq_2_quartiles,
    SUM(CASE WHEN device_code = 'DV-01' THEN new_group_flag ELSE 0 END) AS dv_01_new_group_flag,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    SUM(CASE WHEN device_code = 'DV-03' THEN humidity_share ELSE 0 END) AS dv_03_humidity_share,
    GROUPING_ID() AS grouping_level
FROM with_windows
GROUP BY ROLLUP (device_code, island_id, CASE WHEN row_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING SUM(CASE WHEN device_code = 'DV-03' THEN humidity_share ELSE 0 END) > 0;
