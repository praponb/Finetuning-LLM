WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
)
-- Subtotals with CUBE
SELECT
    quality,
    site_id,
    COUNT(DISTINCT quality) AS distinct_qualitys,
    ROUND(AVG(kwh), 2) AS avg_kwh
FROM valid
GROUP BY CUBE(quality, site_id)
ORDER BY quality NULLS LAST, site_id NULLS LAST
