WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date,
        read_at + INTERVAL 30 MINUTES AS read_at_plus_30m
    FROM meter_readings
    WHERE voltage >= 223.0
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        INITCAP(TRIM(quality)) AS quality_clean,
        INITCAP(TRIM(quality)) AS quality_clean_2
    FROM filtered_readings
    WHERE MONTH(read_at_plus_30m) IN (1, 2)
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.read_at_plus_30m,
        si.zone AS site_zone,
        si.site_name
    FROM derived s
    LEFT JOIN sites si ON si.id = s.site_id
),
without_matches AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM with_details_site s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.zone NOT IN ('Z2', 'Z3')
              AND si.installed_kw > 96.9
        )
)
SELECT
    COALESCE(site_zone, 'ALL') AS site_zone,
    COALESCE(quality, 'ALL') AS quality,
    site_id,
    GROUPING(site_zone) AS is_total_site_zone,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN site_zone = 'Z2' THEN 1 ELSE 0 END) AS n_site_zone_z2
FROM without_matches
GROUP BY ROLLUP(site_zone, quality, site_id)
ORDER BY site_zone NULLS LAST, quality NULLS LAST, site_id NULLS LAST
