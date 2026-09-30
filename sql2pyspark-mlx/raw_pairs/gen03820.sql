WITH filtered AS (
    SELECT *, LEAST(pressure, temp_c) AS least_pressure_temp_c
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
      AND temp_c < 24
)
SELECT
    id,
    reading_ts,
    humidity,
    least_pressure_temp_c
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure, id) AS rank_in_group
    FROM filtered
) x
WHERE rank_in_group = 1;
