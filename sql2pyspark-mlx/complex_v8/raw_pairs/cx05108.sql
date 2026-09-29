WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-02 09:00:00'
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.site_name,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM valid s
    JOIN sites si ON si.id = s.site_id
    WHERE si.site_name IS NOT NULL
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw_2,
        si.site_name AS site_site_name
    FROM with_parent s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw > 93.3
),
joined_site AS (
    -- Join sites
    SELECT s.*, si.installed_kw AS site_installed_kw_3, si.tariff AS site_tariff
    FROM with_details s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT *
FROM (
    SELECT read_at, quality, site_tariff, site_installed_kw
    FROM joined_site
) src
PIVOT (
    AVG(site_installed_kw)
    FOR site_tariff IN ('FLAT', 'TOU', 'EV')
)
ORDER BY read_at, quality
