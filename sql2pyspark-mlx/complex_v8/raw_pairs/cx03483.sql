WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ABS(voltage - 225.2) AS voltage_distance,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE voltage BETWEEN 213.5 AND 234.7
),
with_parent AS (
    -- Join sites
    SELECT
        s.*,
        si.zone AS site_zone,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM cleaned_readings s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(site_zone, 'ALL') AS site_zone,
    COALESCE(quality_group, 'ALL') AS quality_group,
    COALESCE(quality, 'ALL') AS quality,
    GROUPING(site_zone) AS is_total_site_zone,
    SUM(voltage_distance) AS total_voltage_distance
FROM with_parent
GROUP BY ROLLUP(site_zone, quality_group, quality)
ORDER BY site_zone NULLS LAST, quality_group NULLS LAST, quality NULLS LAST
