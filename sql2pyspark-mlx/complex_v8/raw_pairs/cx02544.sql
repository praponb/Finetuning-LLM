WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 1.07, 2) AS voltage_adj,
        LOWER(TRIM(quality)) AS quality_clean
    FROM meter_readings
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.voltage_adj,
        s.quality_clean,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM cleaned_readings s
    JOIN sites si ON si.id = s.site_id
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        voltage,
        quality,
        voltage_adj,
        quality_clean,
        site_installed_kw,
        site_name,
        LENGTH(TRIM(quality_clean)) AS quality_clean_len,
        ROUND(kwh / NULLIF(voltage_adj, 0), 3) AS kwh_per_voltage_adj,
        CONCAT_WS('|', quality, quality_clean) AS quality_quality_clean_label
    FROM with_details
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.voltage_adj,
        s.quality_clean,
        s.site_installed_kw,
        s.quality_clean_len,
        s.kwh_per_voltage_adj,
        s.quality_quality_clean_label,
        si.zone AS site_zone,
        si.site_name AS site_site_name
    FROM enriched s
    JOIN sites si ON si.id = s.site_id
    WHERE si.site_name IS NOT NULL
)
SELECT
    id,
    site_id,
    quality,
    voltage_adj,
    quality_clean,
    kwh_per_voltage_adj,
    site_zone,
    site_site_name
FROM combined_details
ORDER BY kwh_per_voltage_adj DESC NULLS FIRST, site_zone DESC NULLS FIRST
