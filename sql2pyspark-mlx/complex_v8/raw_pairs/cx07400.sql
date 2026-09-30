WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage > 224.9
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(least_voltage_kwh / 50) * 50 AS least_voltage_kwh_bucket,
        COALESCE(least_voltage_kwh, 0.0) AS least_voltage_kwh_filled,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM cleaned_readings
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(least_voltage_kwh_filled) OVER (PARTITION BY quality ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_least_voltage_kwh_filled,
        MAX(kwh) OVER w_quality AS quality_max_kwh,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY quality ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh
    FROM labeled
    WINDOW w_quality AS (PARTITION BY quality)
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage >= 224.9
              AND m.quality NOT IN ('EST', 'BAD')
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, quality, quality_or_unknown, least_voltage_kwh
    FROM qualified
) src
PIVOT (
    SUM(least_voltage_kwh)
    FOR quality_or_unknown IN ('EST', 'OK')
)
