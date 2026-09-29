WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
kept_keys AS (
    -- Set intersection on keys
    SELECT device_code, firmware
    FROM sessionized
    INTERSECT ALL
    SELECT device_code, firmware
    FROM device_readings
    WHERE status_bits BETWEEN 5 AND 8
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    JOIN kept_keys k ON s.device_code = k.device_code AND s.firmware = k.firmware
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(status_bits, 0) AS status_bits_rounded
    FROM kept_rows
    WHERE temp_c BETWEEN 27 AND 35
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        firmware,
        new_group_flag,
        session_id,
        ROUND(pressure / SUM(pressure) OVER (PARTITION BY device_code), 4) AS pressure_share
    FROM filtered
    WHERE HOUR(reading_ts) BETWEEN 9 AND 14
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        device_code,
        reading_ts,
        humidity,
        new_group_flag,
        session_id,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST) AS humidity_cume_dist,
        MIN(new_group_flag) OVER w_recent AS device_code_min_new_group_flag,
        COUNT(pressure_share) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS pressure_share_count_15m
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY device_code)
),
kept_keys_2 AS (
    -- Set intersection on keys
    SELECT device_code
    FROM metrics
    INTERSECT ALL
    SELECT device_code
    FROM device_readings
    WHERE temp_c BETWEEN 22 AND 27
),
kept_rows_device_readings AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN kept_keys_2 k ON s.device_code = k.device_code
)
-- Multi-level aggregation
SELECT
    CASE WHEN session_id >= 6 THEN 'HIGH' ELSE 'LOW' END AS session_id_tier,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list
FROM kept_rows_device_readings
GROUP BY CASE WHEN session_id >= 6 THEN 'HIGH' ELSE 'LOW' END
ORDER BY num_device_codes NULLS FIRST
