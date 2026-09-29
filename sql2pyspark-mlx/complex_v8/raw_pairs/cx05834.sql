WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        QUARTER(read_at) AS read_at_quarter,
        CASE WHEN voltage >= 235.0 THEN 'large' WHEN voltage >= 219.5 THEN 'medium' ELSE 'small' END AS voltage_tier,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE quality NOT IN ('OK', 'BAD')
      AND voltage > 216.5
),
with_details AS (
    -- Join sites
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
),
per_group AS (
    -- Group-level totals
    SELECT
        site_id,
        SUM(read_at_quarter) AS total_read_at_quarter,
        ROUND(100 * COUNT(CASE WHEN LOWER(site_name) LIKE '%ar%' THEN 1 END) / COUNT(*), 1) AS pct_lower_site_name_like_ar,
        ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
        SUM(kwh) AS total_kwh,
        MIN(site_commissioned_on) AS first_site_commissioned_on
    FROM with_details
    GROUP BY site_id
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM per_group s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
      AND s.total_kwh < 540.0
)
-- Final projection
SELECT
    site_id,
    total_read_at_quarter,
    pct_lower_site_name_like_ar,
    site_name_list,
    ARRAY_JOIN(ARRAY_SORT(site_name_list), ',') AS site_name_list_text
FROM qualified
ORDER BY site_name_list_text DESC NULLS FIRST, pct_lower_site_name_like_ar DESC
