WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DAYOFWEEK(reading_ts) AS reading_ts_dayofweek
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 03:00:00'
      AND firmware = '2.0.1'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity, id) AS rank_in_group
        FROM islands
    ) t
    WHERE rank_in_group = 1
)
-- Window metrics over several different partitions
SELECT
    *,
    LEAD(temp_c, 2) OVER (PARTITION BY device_code ORDER BY new_group_flag, id) AS next_temp_c,
    CUME_DIST() OVER (PARTITION BY device_code ORDER BY status_bits DESC) AS status_bits_cume_dist,
    RANK() OVER w_hist AS pressure_rank,
    AVG(temp_c) OVER w_running AS temp_c_avg_3_prior,
    SUM(burst_id) OVER (PARTITION BY device_code ORDER BY reading_ts_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_burst_id
FROM latest
WINDOW w_hist AS (PARTITION BY device_code ORDER BY pressure), w_running AS (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
