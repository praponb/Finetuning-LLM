WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) temp_c_count_1h,
        DENSE_RANK() OVER w_grp humidity_dense_rank
    FROM device_readings
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY humidity)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure NULLS FIRST, id) AS rn
        FROM metrics
    ) x
    WHERE rn = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        temp_c_count_1h,
        rn,
        DATEDIFF(CURRENT_DATE(), reading_ts) / 365.25 years_since_reading_ts
    FROM deduped
    WHERE reading_ts >= TIMESTAMP '2025-09-01 04:00:00'
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(temp_c) OVER w_running next_temp_c,
        LEAD(years_since_reading_ts) OVER w_running next_years_since_reading_ts,
        ROW_NUMBER() OVER w_running event_seq,
        AVG(years_since_reading_ts) OVER w_recent years_since_reading_ts_avg_2h
    FROM cleaned
    WINDOW w_running AS (PARTITION BY device_code ORDER BY temp_c, id), w_recent AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    SUM(next_temp_c) AS total_next_temp_c,
    COUNT(*) AS cnt,
    BIT_AND(status_bits) AS status_bits_bit_and
FROM metrics_device_readings
GROUP BY 1
HAVING total_next_temp_c < 1000000
ORDER BY cnt DESC
