WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('DAY', read_at) AS read_at_day,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE voltage BETWEEN 213.2 AND 240.1
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_day,
        s.read_at_date,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM recent s
    JOIN sites si ON si.id = s.site_id
),
site_id_stats AS (
    -- Max site_installed_kw per site_id
    SELECT site_id, MAX(site_installed_kw) AS max_site_installed_kw
    FROM combined_details
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_site_installed_kw,
        ROUND(s.site_installed_kw - g.max_site_installed_kw, 2) AS site_installed_kw_minus_max
    FROM combined_details s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.site_installed_kw = g.max_site_installed_kw
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.read_at_day,
        s.site_zone,
        s.site_installed_kw,
        si.site_name,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw_2
    FROM compared s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    read_at,
    quality,
    read_at_day,
    site_name,
    site_commissioned_on,
    site_installed_kw_2 + site_installed_kw AS site_installed_kw_2_plus_site_installed_kw
FROM joined
WHERE LENGTH(TRIM(site_name)) >= 7
ORDER BY read_at DESC
