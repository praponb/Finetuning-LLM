WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (kwh > 51.0 OR site_id IS NOT NULL)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        read_at,
        kwh,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        COALESCE(kwh, 0.0) AS kwh_filled,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM base_readings
),
derived AS (
    -- Derive helper columns
    SELECT *, DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM annotated
    WHERE read_at >= TIMESTAMP '2025-02-06 13:00:00'
),
derived_2 AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(days_since_read_at, 0) AS days_since_read_at_filled,
        CONCAT(quality, '#', CAST(read_at_dayofweek AS STRING)) AS quality_ref
    FROM derived
    WHERE days_since_read_at < 525
)
SELECT id, field, value
FROM (
    SELECT id, kwh, kwh_filled
    FROM derived_2
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (kwh, kwh_filled)
) unp
