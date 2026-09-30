WITH gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
valid AS (
    SELECT *, island_id + temp_c AS island_id_plus_temp_c
    FROM sessions_device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND TO_DATE(reading_ts) >= '2025-05-01'
),
latest AS (
    SELECT
        id,
        temp_c,
        pressure,
        firmware,
        island_id
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY island_id, id) = 1
),
cleaned AS (
    SELECT
        *,
        GREATEST(pressure, temp_c) AS greatest_pressure_temp_c,
        ROUND(island_id / pressure, 2) AS island_id_per_pressure
    FROM latest
    WHERE firmware = '1.1.0'
),
scored AS (
    SELECT
        *,
        LAST_VALUE(island_id_per_pressure, TRUE) OVER (ORDER BY greatest_pressure_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_island_id_per_pressure,
        NTILE(4) OVER (ORDER BY pressure DESC, id) AS pressure_bucket
    FROM cleaned
),
with_windows AS (
    SELECT
        id,
        temp_c,
        pressure,
        firmware,
        island_id,
        greatest_pressure_temp_c,
        pressure_bucket,
        LAST_VALUE(pressure_bucket) IGNORE NULLS OVER (ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pressure_bucket,
        AVG(pressure_bucket) OVER (ORDER BY island_id, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS pressure_bucket_avg_3_prior
    FROM scored
),
filtered AS (
    SELECT
        id,
        temp_c,
        pressure,
        firmware,
        island_id,
        greatest_pressure_temp_c,
        pressure_bucket,
        last_known_pressure_bucket,
        pressure_bucket_avg_3_prior,
        ROUND(last_known_pressure_bucket, 1) AS last_known_pressure_bucket_rounded
    FROM with_windows
    WHERE temp_c <= 25
)
SELECT
    island_id,
    SUM(pressure_bucket_avg_3_prior) AS total_pressure_bucket_avg_3_prior,
    PERCENTILE_APPROX(last_known_pressure_bucket_rounded, 0.5) AS last_known_pressure_bucket_rounded_p50
FROM filtered
GROUP BY island_id
ORDER BY total_pressure_bucket_avg_3_prior DESC NULLS LAST;
