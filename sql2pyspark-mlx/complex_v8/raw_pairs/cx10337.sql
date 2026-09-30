WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        CASE WHEN voltage >= 232.0 THEN 'A' WHEN voltage >= 221.8 THEN 'B' ELSE 'C' END AS voltage_tier,
        read_at + INTERVAL 2 HOURS AS read_at_plus_2h,
        DATE_TRUNC('MONTH', read_at) AS read_at_month
    FROM meter_readings
    WHERE voltage <= 235.8
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.read_at_month,
        si.installed_kw AS site_installed_kw,
        si.site_name,
        si.tariff AS site_tariff
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM combined_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage >= 221.4
              AND m.quality = 'OK'
        )
)
SELECT
    id,
    site_id,
    quality,
    read_at_month,
    site_installed_kw,
    site_name,
    site_tariff,
    DATE_FORMAT(read_at_month, 'MMM yyyy') AS read_at_month_label
FROM filtered
