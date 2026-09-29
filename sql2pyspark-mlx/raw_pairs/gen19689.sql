WITH filtered AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        ROUND(humidity, 2) AS humidity_rounded
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
filtered_device_readings AS (
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        firmware,
        CASE WHEN humidity_rounded >= 66 THEN 'High' WHEN humidity_rounded >= 34 THEN 'Medium' ELSE 'Low' END AS humidity_rounded_tier,
        humidity_rounded - temp_c AS humidity_rounded_minus_temp_c
    FROM filtered
    WHERE reading_ts >= TO_DATE('2025-09-01')
      AND firmware = '1.1.0'
),
with_windows AS (
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        AVG(pressure) OVER w_part AS pressure_avg_3,
        NTILE(10) OVER w_grp AS humidity_rounded_minus_temp_c_bucket,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pressure
    FROM filtered_device_readings
    WHERE pressure < 998
    WINDOW w_part AS (PARTITION BY device_code ORDER BY humidity_rounded_minus_temp_c, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY device_code ORDER BY humidity_rounded_minus_temp_c, id)
),
filtered_device_readings_2 AS (
    SELECT *, COALESCE(humidity_rounded_minus_temp_c_bucket, 0) AS humidity_rounded_minus_temp_c_bucket_filled
    FROM with_windows
    WHERE pressure BETWEEN 1000 AND 1006
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity_rounded_minus_temp_c_bucket_filled DESC, id) AS rank_in_group
    FROM filtered_device_readings_2
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    device_code,
    temp_c,
    pressure,
    humidity_rounded_minus_temp_c_bucket,
    running_pressure,
    humidity_rounded_minus_temp_c_bucket_filled
FROM ranked
ORDER BY humidity_rounded_minus_temp_c_bucket_filled DESC NULLS FIRST, device_code DESC;
