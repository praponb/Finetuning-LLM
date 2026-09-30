WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage > 223.3
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM cleaned
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        voltage,
        quality,
        SIZE(COLLECT_SET(quality_or_unknown) OVER (PARTITION BY site_id, quality)) AS distinct_quality_or_unknown_in_site_id_quality,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id, quality), 0), 4) AS kwh_share_of_site_id_quality
    FROM tagged
)
SELECT
    COALESCE(quality, 'ALL') AS quality,
    site_id,
    distinct_quality_or_unknown_in_site_id_quality,
    COUNT(DISTINCT quality) AS distinct_qualitys
FROM metrics
GROUP BY GROUPING SETS ((quality, site_id, distinct_quality_or_unknown_in_site_id_quality), (quality))
ORDER BY quality NULLS LAST, site_id NULLS LAST, distinct_quality_or_unknown_in_site_id_quality NULLS LAST
