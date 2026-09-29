SELECT
    *,
    AVG(pressure) OVER w_hist AS running_avg_pressure,
    MIN(pressure) OVER w_hist AS running_min_pressure,
    COUNT(pressure) OVER w_hist AS running_count_pressure,
    ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share
FROM device_readings
WHERE reading_ts >= '2025-03-01'
WINDOW w_hist AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
