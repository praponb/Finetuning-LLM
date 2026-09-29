WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE voltage > 222.1
)
SELECT id AS ref_id, quality
FROM base
WHERE quality NOT IN ('BAD', 'EST')
INTERSECT
SELECT id, tariff
FROM tariff_windows
