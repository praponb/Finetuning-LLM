WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE (kwh <= 116.8 OR site_id <= 6)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM prepared
    WHERE voltage < (SELECT MAX(voltage) FROM prepared)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        LEAST(voltage, kwh) AS least_voltage_kwh,
        ROUND(voltage * 1.1, 2) AS voltage_adj
    FROM qualified
)
SELECT *
FROM (
    SELECT read_at, quality, voltage_adj
    FROM annotated
) src
PIVOT (
    AVG(voltage_adj)
    FOR quality IN ('OK', 'BAD', 'EST')
)
ORDER BY read_at
