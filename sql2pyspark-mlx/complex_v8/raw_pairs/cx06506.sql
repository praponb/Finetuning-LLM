WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM meter_readings
    WHERE voltage BETWEEN 215.6 AND 232.0
      AND HOUR(read_at) BETWEEN 10 AND 14
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        voltage,
        quality_slug,
        LENGTH(TRIM(quality_slug)) AS quality_slug_len,
        LOWER(REGEXP_REPLACE(TRIM(quality_slug), ' ', '_')) AS quality_slug_slug
    FROM recent
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    quality_slug,
    GREATEST(quality_slug_len, voltage) AS greatest_quality_slug_len_voltage
FROM labeled
