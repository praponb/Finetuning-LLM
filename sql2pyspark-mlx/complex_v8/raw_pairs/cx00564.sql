WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('EST', 'OK') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_group,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff IN ('FLAT', 'TOU')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(voltage) OVER w_all_by_voltage AS next_voltage,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_tariff) AS rows_in_read_at_month_site_tariff,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_tariff), 2) AS voltage_vs_read_at_month_site_tariff_avg
    FROM combined_details
    WINDOW w_all_by_voltage AS (ORDER BY voltage ASC, id)
),
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.quality_group,
        s.site_tariff,
        s.site_commissioned_on,
        s.rows_in_read_at_month_site_tariff,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff_2
    FROM metrics s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality_group,
    site_commissioned_on,
    site_tariff_2
FROM with_details
ORDER BY site_tariff_2, id
LIMIT 10
