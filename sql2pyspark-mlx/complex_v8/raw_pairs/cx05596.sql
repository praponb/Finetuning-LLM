WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-18 07:00:00'
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM cleaned
    WHERE voltage > (SELECT MIN(voltage) FROM cleaned)
)
SELECT *
FROM (
    SELECT site_id, read_at, quality, voltage
    FROM qualified
) src
PIVOT (
    AVG(voltage) AS avg_v, SUM(voltage) AS sum_v
    FOR quality IN ('BAD', 'EST', 'OK')
)
