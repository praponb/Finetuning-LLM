WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        ABS(kwh - 52.4) AS kwh_distance
    FROM meter_readings
),
latest_per_key AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        quality_prefix,
        kwh_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY kwh DESC, id DESC) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
)
-- Subtotals with CUBE
SELECT
    quality,
    site_id,
    GROUPING_ID() AS grouping_level,
    MIN(kwh_distance) AS min_kwh_distance,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
    SUM(CASE WHEN quality <> 'BAD' THEN 1 ELSE 0 END) AS n_quality_bad
FROM latest_per_key
GROUP BY CUBE(quality, site_id)
ORDER BY quality NULLS LAST, site_id NULLS LAST
