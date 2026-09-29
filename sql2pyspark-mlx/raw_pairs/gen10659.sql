WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, humidity * status_bits AS humidity_times_status_bits
    FROM device_readings
    WHERE status_bits BETWEEN 5 AND 9
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        firmware,
        humidity_times_status_bits,
        COUNT(humidity) OVER (PARTITION BY DATE_TRUNC('WEEK', reading_ts) ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS humidity_count_15m,
        NTILE(3) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC) AS temp_c_bucket
    FROM filtered
    WHERE status_bits < 6
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
scored_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        reading_ts,
        humidity_times_status_bits,
        humidity_count_15m,
        AVG(humidity_count_15m) OVER (ORDER BY humidity_count_15m ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS humidity_count_15m_avg_3_prior,
        SUM(temp_c_bucket) OVER (PARTITION BY YEAR(reading_ts), device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS temp_c_bucket_sum_15m
    FROM deduped
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(temp_c_bucket_sum_15m) OVER w_grp AS last3_min_temp_c_bucket_sum_15m,
        MAX(temp_c_bucket_sum_15m) OVER w_grp AS last3_max_temp_c_bucket_sum_15m,
        COUNT(temp_c_bucket_sum_15m) OVER w_grp AS last3_count_temp_c_bucket_sum_15m,
        SUM(temp_c_bucket_sum_15m) OVER w_grp AS last3_sum_temp_c_bucket_sum_15m
    FROM scored_device_readings
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) AS prev_reading_ts
    FROM with_windows
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity_times_status_bits,
        humidity_count_15m,
        humidity_count_15m_avg_3_prior,
        temp_c_bucket_sum_15m,
        last3_min_temp_c_bucket_sum_15m,
        last3_max_temp_c_bucket_sum_15m,
        last3_count_temp_c_bucket_sum_15m,
        last3_sum_temp_c_bucket_sum_15m,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM islands
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps_2
)
SELECT
    id,
    device_code,
    reading_ts,
    humidity_times_status_bits,
    humidity_count_15m_avg_3_prior,
    last3_min_temp_c_bucket_sum_15m,
    last3_count_temp_c_bucket_sum_15m,
    last3_sum_temp_c_bucket_sum_15m,
    session_no,
    CASE WHEN device_code RLIKE '^(DV-02|DV-01)$' THEN 'EXTENDED' WHEN device_code RLIKE '^(DV-03|DV-04)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS device_code_zone
FROM sessionized
ORDER BY id DESC
