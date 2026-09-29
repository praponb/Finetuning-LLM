WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
with_parent_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.installed_kw AS site_installed_kw, si.tariff AS site_tariff
    FROM scoped_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff NOT IN ('FLAT', 'EV')
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        kwh,
        quality,
        site_installed_kw,
        ROUND(site_installed_kw * 0.25, 2) AS site_installed_kw_adj
    FROM with_parent_site
    WHERE site_tariff <> 'TOU'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM scored s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_parent_site_2 AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.site_installed_kw,
        s.site_installed_kw_adj,
        si.commissioned_on AS site_commissioned_on,
        si.site_name,
        si.zone AS site_zone
    FROM with_counts s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT *
FROM (
    SELECT quality, site_commissioned_on, site_zone, kwh
    FROM with_parent_site_2
) src
PIVOT (
    COUNT(kwh) AS count_v, SUM(kwh) AS sum_v
    FOR site_zone IN ('Z2', 'Z3')
)
