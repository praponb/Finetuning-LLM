WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        LENGTH(TRIM(quality)) AS quality_len,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
    FROM meter_readings
    WHERE quality <> 'BAD'
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered_readings
    WHERE voltage > (SELECT MIN(voltage) FROM filtered_readings)
)
SELECT *
FROM (
    SELECT site_id, quality_len, quality, kwh
    FROM screened
) src
PIVOT (
    MAX(kwh)
    FOR quality IN ('BAD', 'OK')
)
