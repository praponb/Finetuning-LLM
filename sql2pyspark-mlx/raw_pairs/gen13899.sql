WITH base AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        firmware,
        YEAR(reading_ts) reading_ts_year
    FROM device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 08:00:00')
)
SELECT
    id,
    firmware,
    LEAD(humidity, 2) OVER (ORDER BY reading_ts_year, id) next_humidity,
    SUM(reading_ts_year) OVER (PARTITION BY DATE_TRUNC('week', reading_ts), device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_reading_ts_year
FROM base;
