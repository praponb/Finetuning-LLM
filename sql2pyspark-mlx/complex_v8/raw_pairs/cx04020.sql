WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DAYOFWEEK(read_at) AS read_at_dayofweek,
        DATE_TRUNC('MONTH', read_at) AS read_at_month
    FROM meter_readings
    WHERE voltage >= 219.6
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.read_at_dayofweek,
        s.read_at_month,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on BETWEEN DATE '2024-09-15' AND DATE '2025-05-10'
),
enriched_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.read_at_dayofweek,
        s.read_at_month,
        s.site_name,
        si.commissioned_on AS site_commissioned_on_2,
        si.site_name AS site_site_name
    FROM enriched s
    JOIN sites si ON si.id = s.site_id
),
enriched_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.read_at_dayofweek,
        s.site_name,
        s.site_site_name,
        si.installed_kw AS site_installed_kw,
        si.site_name AS site_site_name_2
    FROM enriched_2 s
    JOIN sites si ON si.id = s.site_id
),
latest_per_key AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at_dayofweek,
        site_name,
        site_site_name,
        site_installed_kw,
        site_site_name_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at_dayofweek DESC, id DESC) AS rn
        FROM enriched_site
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    voltage,
    site_name,
    site_installed_kw,
    site_site_name_2
FROM latest_per_key
ORDER BY read_at, id DESC
LIMIT 5
