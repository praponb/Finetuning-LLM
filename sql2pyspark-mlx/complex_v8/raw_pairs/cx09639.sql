WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        kwh - voltage AS kwh_minus_voltage,
        kwh + voltage AS kwh_plus_voltage
    FROM meter_readings
    WHERE voltage < 233.8
      AND site_id <= 5
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        kwh_minus_voltage,
        kwh_plus_voltage,
        ROUND(kwh * 1.07, 2) AS kwh_adj
    FROM scoped
    WHERE quality IS NOT NULL
),
annotated AS (
    -- Add calculated columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM labeled
)
SELECT
    quality,
    site_id,
    ROUND(SUM(kwh), 2) AS total_kwh,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(kwh_adj) AS min_kwh_adj
FROM annotated
GROUP BY GROUPING SETS ((quality, site_id), (quality), ())
ORDER BY quality NULLS LAST, site_id NULLS LAST
