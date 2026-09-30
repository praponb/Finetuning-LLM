WITH base AS (
    SELECT
        id,
        device_code,
        pressure,
        firmware,
        ROUND(pressure * 0.85, 2) AS adjusted_pressure
    FROM device_readings
    WHERE device_code IN ('DV-03', 'DV-01')
),
filtered AS (
    SELECT
        id,
        device_code,
        pressure,
        firmware,
        adjusted_pressure,
        CASE WHEN firmware IN ('1.1.0', '2.0.1') THEN 'Core' ELSE 'Other' END AS firmware_group
    FROM base
    WHERE pressure BETWEEN 996 AND 1005
),
scored AS (
    SELECT
        *,
        COUNT(pressure) OVER w_running AS running_count_pressure,
        SUM(pressure) OVER w_running AS running_sum_pressure,
        MIN(pressure) OVER w_running AS running_min_pressure,
        MAX(pressure) OVER w_running AS running_max_pressure,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure, id) AS row_seq
    FROM filtered
    WHERE firmware NOT IN ('2.0.1', '1.1.0')
    WINDOW w_running AS (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        *,
        MAX(running_count_pressure) OVER (PARTITION BY device_code ORDER BY running_min_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_count_pressure,
        MIN(running_count_pressure) OVER (PARTITION BY device_code ORDER BY running_min_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_running_count_pressure,
        SUM(running_count_pressure) OVER (PARTITION BY device_code ORDER BY running_min_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_count_pressure,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure
    FROM scored
),
base_device_readings AS (
    SELECT *, CASE WHEN running_max_running_count_pressure >= 14.5 THEN 'High' WHEN running_max_running_count_pressure >= 4.8 THEN 'Medium' ELSE 'Low' END AS running_max_running_count_pressure_tier
    FROM metrics
    WHERE running_min_pressure BETWEEN 1004 AND 1011
)
SELECT
    CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    CASE WHEN cumulative_pressure >= 4665 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_pressure_tier,
    COUNT(*) FILTER (WHERE device_code = 'DV-04') AS dv_04_rows,
    ROUND(SUM(adjusted_pressure) / NULLIF(COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END), 0), 2) AS adjusted_pressure_per_dv_03,
    COUNT(*) FILTER (WHERE device_code = 'DV-03') AS dv_03_rows,
    COUNT(DISTINCT firmware) AS distinct_firmwares
FROM base_device_readings
GROUP BY 1, 2;
