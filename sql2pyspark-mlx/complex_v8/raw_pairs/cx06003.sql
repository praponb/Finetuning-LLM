WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE voltage >= 220.7
      AND kwh < 109.3
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        site_installed_kw,
        site_zone,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label
    FROM joined_site
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    site_installed_kw
FROM annotated
WHERE kwh <= 109.0
ORDER BY id
