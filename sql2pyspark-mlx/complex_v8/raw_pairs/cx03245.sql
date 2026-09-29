WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 10 AND 14
),
with_parent_site AS (
    -- Join sites
    SELECT s.*, si.zone AS site_zone, si.site_name
    FROM scoped s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY site_zone, site_id) AS rows_in_site_zone_site_id,
        LEAD(read_at, 3) OVER w_quality_site_id_by_kwh AS next3_read_at
    FROM with_parent_site
    WINDOW w_quality_site_id_by_kwh AS (PARTITION BY quality, site_id ORDER BY kwh DESC, id)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    site_zone,
    rows_in_site_zone_site_id,
    FLOOR(kwh / 25) * 25 AS kwh_bucket,
    DATEDIFF(DATE '2025-12-31', next3_read_at) AS days_since_next3_read_at
FROM with_trends
ORDER BY rows_in_site_zone_site_id
