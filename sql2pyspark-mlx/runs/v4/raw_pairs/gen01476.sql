WITH metrics_long AS (
    SELECT *
    FROM (
        SELECT
            id,
            device_code,
            temp_c,
            pressure
        FROM device_readings
    ) src
    UNPIVOT INCLUDE NULLS (
        value FOR metric_name IN (temp_c, pressure AS PRESSURE_VAL)
    )
)
SELECT
    device_code,
    COUNT(CASE WHEN metric_name = 'temp_c' THEN 1 END) AS temp_c_count,
    COUNT(*) FILTER (WHERE metric_name = 'temp_c') AS temp_c_rows
FROM metrics_long
GROUP BY device_code
ORDER BY device_code DESC
