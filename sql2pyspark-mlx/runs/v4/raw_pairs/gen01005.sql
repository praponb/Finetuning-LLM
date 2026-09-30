WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        TO_DATE(reading_ts) AS reading_ts_date
    FROM device_readings
    WHERE humidity BETWEEN 59 AND 70
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        reading_ts_date,
        ROUND(pressure / SUM(pressure) OVER (PARTITION BY device_code), 4) AS pressure_share,
        FIRST_VALUE(pressure) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure,
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share,
        FIRST_VALUE(pressure, TRUE) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure_2
    FROM cleaned
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure_share DESC NULLS FIRST, id) AS rn
        FROM metrics
    ) t
    WHERE rn = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    ROUND(AVG(pressure_share), 2) AS avg_pressure_share,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    COUNT_IF(device_code = 'DV-01') AS dv_01_events
FROM deduped
GROUP BY DATE_TRUNC('HOUR', reading_ts)
