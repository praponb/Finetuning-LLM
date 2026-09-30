WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality IN ('BAD', 'EST')
      AND kwh <= 80.8
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        site_id,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        MIN(read_at) AS first_read_at,
        MAX(read_at) AS last_read_at,
        ROUND(SUM(voltage), 2) AS total_voltage
    FROM scoped
    WHERE voltage < 229.1
    GROUP BY DATE_TRUNC('MONTH', read_at), site_id
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.read_at_month,
        s.site_id,
        s.quality_list,
        s.first_read_at,
        s.last_read_at,
        s.total_voltage,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff
    FROM grouped s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone IN ('Z3', 'Z4')
),
joined_site AS (
    -- Attach site attributes
    SELECT
        s.read_at_month,
        s.site_id,
        s.quality_list,
        s.first_read_at,
        s.last_read_at,
        s.total_voltage,
        s.site_zone,
        s.site_installed_kw,
        s.site_tariff,
        si.installed_kw AS site_installed_kw_2
    FROM with_parent_site s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT *
FROM (
    SELECT site_id, first_read_at, site_zone, total_voltage
    FROM joined_site
) src
PIVOT (
    MIN(total_voltage)
    FOR site_zone IN ('Z3', 'Z2')
)
