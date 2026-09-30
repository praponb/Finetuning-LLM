WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        kwh,
        voltage,
        quality,
        YEAR(read_at) AS read_at_year
    FROM prepared
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE voltage > (SELECT AVG(voltage) FROM scored)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE kwh > (SELECT AVG(kwh) FROM filtered)
)
-- Final projection
SELECT
    id,
    kwh,
    voltage,
    read_at_year,
    COALESCE(voltage, 0.0) AS voltage_filled,
    COALESCE(kwh, 0.0) AS kwh_filled
FROM screened
ORDER BY voltage_filled DESC, id
LIMIT 10
