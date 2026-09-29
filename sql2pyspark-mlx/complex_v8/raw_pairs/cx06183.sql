WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE site_id >= 8
      AND kwh >= 29.2
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM recent s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.quality IS NOT NULL
        )
      AND s.kwh_filled < 108.0
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        quality_slug,
        kwh_filled,
        MIN(voltage) OVER (PARTITION BY site_id, quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY kwh DESC), 3) AS kwh_cume_dist
    FROM screened
)
SELECT
    id,
    site_id,
    voltage,
    min_voltage_so_far,
    distinct_quality_in_site_id
FROM with_windows
ORDER BY distinct_quality_in_site_id DESC, voltage DESC, id
LIMIT 5
