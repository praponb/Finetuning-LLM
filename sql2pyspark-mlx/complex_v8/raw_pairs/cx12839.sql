WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE site_id <= 4
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.*,
        si.zone AS site_zone,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM base s
    LEFT JOIN sites si ON si.id = s.site_id
),
latest_per_key AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        days_since_read_at,
        site_zone,
        site_name,
        site_commissioned_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY site_commissioned_on DESC, id DESC) AS row_num
        FROM with_parent_site
    ) ranked
    WHERE row_num = 1
),
joined AS (
    -- Attach site attributes
    SELECT s.*, si.installed_kw AS site_installed_kw, si.tariff AS site_tariff
    FROM latest_per_key s
    LEFT JOIN sites si ON si.id = s.site_id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        DAYOFWEEK(site_commissioned_on) AS site_commissioned_on_dayofweek,
        FLOOR(site_installed_kw / 100) * 100 AS site_installed_kw_bucket
    FROM joined
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    site_zone,
    site_name,
    site_commissioned_on,
    voltage_per_kwh,
    site_commissioned_on_dayofweek,
    site_installed_kw_bucket,
    INITCAP(TRIM(site_tariff)) AS site_tariff_clean
FROM enriched
WHERE UPPER(TRIM(site_name)) LIKE 'D%'
