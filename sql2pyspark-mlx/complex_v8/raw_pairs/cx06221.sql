WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        TO_DATE(read_at) AS read_at_date,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE quality = 'BAD'
      AND read_at >= TIMESTAMP '2025-01-25 11:00:00'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality_slug,
        NTILE(10) OVER (PARTITION BY site_id ORDER BY voltage, id) AS voltage_decile,
        MAX(voltage) OVER (PARTITION BY site_id) AS site_id_max_voltage,
        NTILE(5) OVER (PARTITION BY site_id ORDER BY voltage, id) AS voltage_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY voltage, id), 3) AS voltage_cume_dist
    FROM filtered_readings
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality_slug,
        s.voltage_decile,
        s.site_id_max_voltage,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM windowed s
    JOIN sites si ON si.id = s.site_id
),
latest_per_key AS (
    -- Keep the latest row per site_zone, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality_slug,
        voltage_decile,
        site_id_max_voltage,
        site_installed_kw,
        site_zone
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_zone, site_id ORDER BY voltage_decile DESC, id DESC) AS recency_rank
        FROM with_details_site
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    voltage,
    voltage_decile,
    site_id_max_voltage,
    site_installed_kw,
    site_zone,
    UPPER(TRIM(quality_slug)) AS quality_slug_clean,
    kwh - site_id_max_voltage AS kwh_minus_site_id_max_voltage
FROM latest_per_key
WHERE quality_slug IS NOT NULL
