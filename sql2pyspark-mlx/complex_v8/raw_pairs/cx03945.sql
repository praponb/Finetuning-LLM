WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE kwh >= 16.5
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)) AS read_at_month_min_kwh,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)) AS rows_in_read_at_month,
        MAX(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)) AS read_at_month_max_voltage,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at))) AS distinct_site_id_in_read_at_month
    FROM recent
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.rows_in_read_at_month,
        s.read_at_month_max_voltage,
        s.distinct_site_id_in_read_at_month,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM ranked s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    voltage,
    rows_in_read_at_month,
    read_at_month_max_voltage,
    distinct_site_id_in_read_at_month,
    reading_quality,
    reading_kwh
FROM exploded_detail_readings
ORDER BY kwh DESC, read_at_month_max_voltage DESC
