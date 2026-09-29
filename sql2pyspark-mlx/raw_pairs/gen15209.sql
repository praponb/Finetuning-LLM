WITH base AS (
    SELECT *, DAYOFWEEK(reading_ts) AS reading_ts_dayofweek
    FROM device_readings
    WHERE pressure > 1007
),
matched_keys AS (
    SELECT firmware, id
    FROM base
    INTERSECT ALL
    SELECT firmware, id
    FROM device_readings
    WHERE pressure < 1027
),
kept_rows AS (
    SELECT s.*
    FROM base s
    JOIN matched_keys k ON s.firmware = k.firmware AND s.id = k.id
),
scored AS (
    SELECT
        *,
        MAX(temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS temp_c_max_1h,
        SUM(status_bits) OVER w_ordered AS status_bits_sum_1h,
        COUNT(pressure) OVER w_running AS pressure_count_30m
    FROM kept_rows
    WINDOW w_ordered AS (PARTITION BY MONTH(reading_ts), device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
valid AS (
    SELECT
        *,
        COALESCE(temp_c, 0) AS temp_c_filled,
        humidity - temp_c_max_1h AS humidity_minus_temp_c_max_1h
    FROM scored
    WHERE reading_ts > TIMESTAMP '2025-09-01 07:00:00'
),
scored_device_readings AS (
    SELECT *, MAX(reading_ts_dayofweek) OVER w_recent AS reading_ts_dayofweek_max_2h
    FROM valid
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
)
SELECT
    device_code,
    COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows,
    COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END) AS dv_02_count,
    SUM(CASE WHEN device_code = 'DV-04' THEN reading_ts_dayofweek ELSE 0 END) AS dv_04_reading_ts_dayofweek
FROM scored_device_readings
GROUP BY 1;
