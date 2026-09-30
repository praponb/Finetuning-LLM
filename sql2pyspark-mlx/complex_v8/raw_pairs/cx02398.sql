WITH cleaned_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(kwh, 0.0) AS kwh_filled,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM cleaned_readings
    WHERE quality = 'OK'
)
SELECT
    id,
    site_id,
    kwh,
    quality,
    kwh_filled,
    CASE WHEN kwh_filled >= 78.0 THEN 'A' WHEN kwh_filled >= 24.0 THEN 'B' ELSE 'C' END AS kwh_filled_tier,
    ROUND(kwh_filled / NULLIF(kwh, 0), 3) AS kwh_filled_per_kwh
FROM scored
ORDER BY site_id DESC
