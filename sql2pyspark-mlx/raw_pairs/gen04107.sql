WITH gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessionized AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
matched_keys AS (
    SELECT device_code, id
    FROM sessionized
    INTERSECT ALL
    SELECT device_code, id
    FROM device_readings
    WHERE status_bits <= 11
),
reconciled AS (
    SELECT s.*
    FROM sessionized s
    JOIN matched_keys k ON s.device_code = k.device_code AND s.id = k.id
),
valid AS (
    SELECT *, CAST(reading_ts AS DATE) reading_ts_day
    FROM reconciled
    WHERE reading_ts >= TIMESTAMP '2025-09-01 07:00:00'
),
filtered AS (
    SELECT
        id,
        humidity,
        pressure,
        reading_ts_day,
        CAST(reading_ts AS DATE) reading_ts_day_2,
        CASE WHEN island_id >= 6 THEN 'High' WHEN island_id >= 2 THEN 'Medium' ELSE 'Low' END island_id_tier
    FROM valid
    WHERE humidity BETWEEN 43 AND 45
),
totals AS (
    SELECT
        CASE WHEN pressure >= 1028 THEN 'HIGH' ELSE 'LOW' END AS pressure_tier,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        CORR(humidity, pressure) AS corr_humidity_pressure,
        ROUND(AVG(pressure), 2) AS avg_pressure,
        CORR(humidity, pressure) AS corr_humidity_pressure_2
    FROM filtered
    GROUP BY 1
    HAVING COUNT(*) > 1
)
SELECT
    pressure_tier,
    cnt,
    distinct_ids,
    avg_pressure,
    corr_humidity_pressure_2
FROM totals;
