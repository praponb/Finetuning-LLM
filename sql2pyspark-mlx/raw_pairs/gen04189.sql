SELECT
    id,
    device_code,
    temp_c,
    humidity,
    pressure,
    firmware,
    MIN(pressure) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_min_pressure,
    AVG(pressure) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_pressure,
    COUNT(pressure) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_pressure,
    SUM(pressure) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_pressure,
    SUM(pressure) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_pressure
FROM device_readings
