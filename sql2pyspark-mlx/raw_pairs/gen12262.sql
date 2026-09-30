WITH valid AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CAST(pressure AS INT) AS pressure_int
    FROM device_readings
    WHERE device_code != 'DV-04'
),
enriched AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        MIN(pressure) OVER w_part AS last3_min_pressure,
        COUNT(pressure) OVER w_part AS last3_count_pressure,
        MAX(pressure) OVER w_part AS last3_max_pressure,
        SUM(pressure) OVER w_part AS last3_sum_pressure,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share
    FROM valid
    WHERE temp_c >= 21
    WINDOW w_part AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), reading_ts) AS days_since_reading_ts
    FROM enriched
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 02:00:00')
),
flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
matched_keys AS (
    SELECT device_code
    FROM islands
    EXCEPT ALL
    SELECT device_code
    FROM device_readings
    WHERE device_code = 'DV-01'
),
reconciled AS (
    SELECT s.*
    FROM islands s
    JOIN matched_keys k ON s.device_code = k.device_code
)
SELECT
    temp_c,
    last3_min_pressure,
    last3_count_pressure,
    last3_max_pressure,
    last3_sum_pressure,
    humidity_share,
    days_since_reading_ts,
    session_no
FROM reconciled
ORDER BY humidity_share NULLS FIRST
