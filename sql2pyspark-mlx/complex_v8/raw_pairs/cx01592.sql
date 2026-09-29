WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 0.9, 2) AS kwh_adj
    FROM meter_readings
    WHERE voltage < 241.8
)
SELECT quality, read_at AS happened_at
FROM meter_readings
UNION ALL
SELECT quality, read_at AS happened_at
FROM meter_readings
ORDER BY quality
