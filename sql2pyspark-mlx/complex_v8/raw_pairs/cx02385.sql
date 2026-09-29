WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 7 AND 9
      AND voltage >= 216.1
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        greatest_kwh_voltage,
        ABS(kwh - 52.5) AS kwh_distance,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown_2
    FROM prepared_readings
),
without_matches AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw < 284.0
              AND si.zone IS NOT NULL
        )
)
SELECT *
FROM (
    SELECT quality_or_unknown_2, site_id, quality_or_unknown, voltage
    FROM without_matches
) src
PIVOT (
    MIN(voltage) AS min_v, MAX(voltage) AS max_v
    FOR quality_or_unknown IN ('BAD', 'EST', 'UNKNOWN')
)
