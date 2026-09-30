WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND quality IN ('EST', 'BAD')
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.quality_slug,
        si.zone AS site_zone
    FROM recent s
    LEFT JOIN sites si ON si.id = s.site_id
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_parent s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.kwh <= 92.5
        )
),
latest_per_key AS (
    -- Keep the latest row per site_zone, site_id
    SELECT
        id,
        site_id,
        kwh,
        quality,
        quality_slug,
        site_zone
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_zone, site_id ORDER BY kwh DESC, id DESC) AS rn
        FROM qualified
    ) ranked
    WHERE rn = 1
)
SELECT
    site_id,
    site_zone,
    quality,
    GROUPING(site_id) AS is_total_site_id,
    ROUND(100 * COUNT(CASE WHEN site_zone <> 'Z3' THEN 1 END) / COUNT(*), 1) AS pct_site_zone_z3,
    COUNT(*) AS row_count,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
FROM latest_per_key
GROUP BY GROUPING SETS ((site_id, site_zone, quality), (site_id), (site_zone), ())
ORDER BY site_id NULLS LAST, site_zone NULLS LAST, quality NULLS LAST
