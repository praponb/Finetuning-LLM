WITH filtered AS (
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        ROUND(pressure, 0) AS pressure_rounded,
        humidity * temp_c AS humidity_times_temp_c
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 07:00:00'
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure_rounded DESC, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
metrics AS (
    SELECT
        *,
        MAX(humidity) OVER w_ordered AS running_max_humidity,
        COUNT(humidity) OVER w_ordered AS running_count_humidity,
        AVG(humidity) OVER w_ordered AS running_avg_humidity,
        SUM(humidity) OVER w_ordered AS running_sum_humidity
    FROM latest
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY pressure_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        *,
        SUM(running_max_humidity) OVER (ORDER BY pressure_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_max_humidity,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure_rounded, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY humidity_times_temp_c, id) AS event_seq,
        SUM(running_sum_humidity) OVER (PARTITION BY device_code ORDER BY running_avg_humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_sum_humidity
    FROM metrics
)
SELECT
    id,
    device_code,
    temp_c,
    humidity,
    status_bits,
    firmware,
    pressure_rounded,
    humidity_times_temp_c,
    row_num
FROM with_windows;
