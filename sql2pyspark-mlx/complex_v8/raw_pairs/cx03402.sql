WITH base AS (
    -- Filter sites
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE site_name IS NOT NULL
      AND installed_kw < 386.6
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(zone, '#', CAST(id AS STRING)) AS zone_ref,
        LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
    FROM base
    WHERE commissioned_on >= DATE '2024-10-28'
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        zone,
        commissioned_on,
        zone_ref,
        zone_slug,
        ABS(installed_kw - 284.0) AS installed_kw_distance,
        CASE WHEN installed_kw >= 373.3 THEN 'A' WHEN installed_kw >= 141.6 THEN 'B' ELSE 'C' END AS installed_kw_tier,
        DATE_FORMAT(commissioned_on, 'MMM yyyy') AS commissioned_on_label
    FROM derived
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.id) AS last_reading_read_at
    FROM enriched s
)
SELECT
    zone,
    installed_kw_tier,
    PERCENTILE_APPROX(installed_kw_distance, 0.5) AS median_installed_kw_distance,
    COUNT(*) AS n_rows,
    ARRAY_SORT(COLLECT_SET(zone)) AS zone_list
FROM with_stats
GROUP BY GROUPING SETS ((zone, installed_kw_tier), (zone))
ORDER BY zone NULLS LAST, installed_kw_tier NULLS LAST
