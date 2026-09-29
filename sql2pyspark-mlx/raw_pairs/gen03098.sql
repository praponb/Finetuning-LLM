WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        firmware,
        status_bits + pressure AS status_bits_plus_pressure
    FROM device_readings
    WHERE reading_ts >= TO_DATE('2025-09-01')
),
cleaned_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, WEEKOFYEAR(reading_ts) AS reading_ts_weekofyear
    FROM cleaned
    WHERE reading_ts > DATE '2025-09-01'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY reading_ts_weekofyear DESC) AS dedup_rank
        FROM cleaned_device_readings
    ) x
    WHERE dedup_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(status_bits_plus_pressure) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC) AS next_status_bits_plus_pressure,
        LAG(status_bits_plus_pressure) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC) AS prev_status_bits_plus_pressure,
        LEAD(reading_ts_weekofyear) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC) AS next_reading_ts_weekofyear,
        LAG(reading_ts_weekofyear) OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC) AS prev_reading_ts_weekofyear,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity ASC, id ASC) AS row_seq
    FROM latest
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    AVG(prev_reading_ts_weekofyear) FILTER (WHERE device_code = 'DV-04') AS dv_04_avg_prev_reading_ts_weekofyear,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END) AS dv_03_count
FROM scored
GROUP BY 1
ORDER BY distinct_firmwares ASC
