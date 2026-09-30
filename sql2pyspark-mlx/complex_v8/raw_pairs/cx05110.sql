WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE voltage < 238.3
      AND quality = 'OK'
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.quality_group,
        si.zone AS site_zone
    FROM valid s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    site_id,
    COALESCE(quality_group, 'ALL') AS quality_group,
    GROUPING(site_id) AS is_total_site_id,
    MIN(kwh) AS min_kwh
FROM with_parent
GROUP BY GROUPING SETS ((site_id, quality_group), (site_id))
ORDER BY site_id NULLS LAST, quality_group NULLS LAST
