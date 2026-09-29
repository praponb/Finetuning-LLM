WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(status_bits, humidity) OVER (PARTITION BY device_code) AS corr_status_bits_humidity,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS status_bits_bucket
    FROM sessions_device_readings
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY burst_id DESC, id) AS rank_in_group
        FROM enriched
    ) ranked
    WHERE rank_in_group = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(is_new_burst, 2, 0) OVER w_part AS prev_is_new_burst,
        NTILE(5) OVER (ORDER BY humidity, id) AS humidity_bucket,
        ROW_NUMBER() OVER w_hist AS event_seq,
        LEAD(humidity, 1) OVER w_part AS next_humidity,
        ROW_NUMBER() OVER w_recent AS event_seq_2
    FROM ranked
    WINDOW w_part AS (PARTITION BY device_code ORDER BY corr_status_bits_humidity, id), w_hist AS (PARTITION BY device_code ORDER BY status_bits_bucket DESC, id), w_recent AS (ORDER BY status_bits_bucket DESC, id)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM scored
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM flagged
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        is_new_burst,
        burst_id,
        corr_status_bits_humidity,
        rank_in_group,
        prev_is_new_burst,
        humidity_bucket,
        event_seq,
        next_humidity,
        event_seq_2,
        burst_id_2,
        ROUND(temp_c * 1.15, 2) AS temp_c_scaled
    FROM sessionized
    WHERE reading_ts > TIMESTAMP '2025-09-01 10:00:00'
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(temp_c_scaled AS INT) AS temp_c_scaled_int
    FROM cleaned
    WHERE reading_ts > DATE '2025-09-01'
      AND burst_id <= 3
)
-- Running and moving aggregates
SELECT
    *,
    AVG(event_seq) OVER (PARTITION BY device_code ORDER BY prev_is_new_burst, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_seq_avg_6_prior,
    FIRST_VALUE(temp_c_scaled_int) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_temp_c_scaled_int,
    LEAD(event_seq_2, 2) OVER (PARTITION BY device_code ORDER BY temp_c_scaled, id) AS next_event_seq_2
FROM base
