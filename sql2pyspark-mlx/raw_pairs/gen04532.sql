WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM sessionized
),
sessionized_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        session_id,
        SUM(island_id) OVER w_hist AS island_id_sum_30m,
        AVG(is_new_burst) OVER (PARTITION BY device_code) AS device_code_avg_is_new_burst,
        LEAD(is_new_burst, 1) OVER (PARTITION BY device_code ORDER BY session_id, id) AS next_is_new_burst,
        AVG(session_id) OVER w_hist AS session_id_avg_30m
    FROM sessionized_device_readings
    WHERE TO_DATE(reading_ts) < '2024-06-25'
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY island_id_sum_30m DESC NULLS FIRST, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM ranked
),
flagged_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized_device_readings_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged_2
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY dedup_rank DESC, id) AS latest_rank
        FROM sessionized_device_readings_2
    ) ranked
    WHERE latest_rank = 1
),
daily AS (
    -- Multi-level aggregation
    SELECT
        session_id,
        MIN(device_code_avg_is_new_burst) AS min_device_code_avg_is_new_burst,
        PERCENTILE_APPROX(island_id_sum_30m, 0.95) AS island_id_sum_30m_p95,
        COUNT(DISTINCT device_code) AS distinct_device_codes,
        MAX(is_new_session) AS max_is_new_session
    FROM latest
    GROUP BY session_id
)
SELECT
    session_id,
    min_device_code_avg_is_new_burst,
    distinct_device_codes,
    max_is_new_session
FROM daily;
