WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
)
SELECT
    quality,
    site_id,
    COUNT(*) AS cnt,
    ROUND(100 * COUNT(CASE WHEN quality IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_quality_is_not_null,
    COUNT(DISTINCT site_id) AS distinct_site_ids
FROM base_readings
GROUP BY ROLLUP(quality, site_id)
