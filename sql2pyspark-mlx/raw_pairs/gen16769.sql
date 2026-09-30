WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        DATEDIFF(CURRENT_DATE(), reading_ts) AS days_since_reading_ts,
        temp_c + pressure AS temp_c_plus_pressure
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 01:00:00'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(status_bits) OVER (PARTITION BY MONTH(reading_ts), device_code ORDER BY session_no ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS status_bits_avg_2,
        LEAD(pressure, 1) OVER w_hist AS next_pressure,
        AVG(temp_c) OVER (ORDER BY humidity ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS temp_c_avg_5_prior
    FROM sessionized
    WINDOW w_hist AS (PARTITION BY YEAR(reading_ts), device_code ORDER BY days_since_reading_ts ASC, id ASC)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY new_group_flag ASC NULLS LAST) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(rank_in_group, 1) AS rank_in_group_rounded
    FROM deduped
    WHERE humidity <= 40
)
SELECT
    id,
    reading_ts,
    humidity,
    firmware,
    temp_c_plus_pressure,
    new_group_flag,
    session_no,
    status_bits_avg_2,
    next_pressure
FROM cleaned
ORDER BY id DESC NULLS LAST, new_group_flag DESC;
