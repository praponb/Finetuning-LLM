WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE site_id <= 5
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(quality_clean), 1, 4) AS quality_clean_prefix,
        ROUND(kwh * 0.25, 2) AS kwh_adj
    FROM cleaned_readings
)
-- Final projection
SELECT
    id,
    voltage,
    quality,
    quality_clean,
    quality_clean_prefix,
    kwh_adj
FROM tagged
