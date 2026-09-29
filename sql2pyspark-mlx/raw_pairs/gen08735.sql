WITH valid AS (
    SELECT
        *,
        COALESCE(humidity, 0) AS humidity_filled,
        LOWER(firmware) AS firmware_lower
    FROM device_readings
    WHERE firmware = '2.0.1'
),
gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessionized AS (
    SELECT
        id,
        device_code,
        reading_ts,
        pressure,
        status_bits,
        firmware,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    PERCENTILE_APPROX(status_bits, 0.5) AS status_bits_p50,
    SUM(CASE WHEN device_code = 'DV-03' THEN island_id ELSE 0 END) AS dv_03_island_id,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(pressure, 0.9) AS pressure_p90
FROM sessionized
GROUP BY DATE_TRUNC('HOUR', reading_ts)
HAVING row_count > 1
ORDER BY hour_window DESC;
