WITH scoped_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw
    FROM sites
    WHERE UPPER(TRIM(site_name)) LIKE 'H%'
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_name,
        s.installed_kw,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM scoped_sites s
    LEFT JOIN meter_readings m ON m.site_id = s.id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, ROUND(AVG(voltage), 2) AS avg_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_voltage
    FROM exploded_detail s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        installed_kw,
        reading_id,
        reading_read_at,
        reading_quality,
        reading_kwh,
        avg_reading_voltage,
        SUM(avg_reading_voltage) OVER (PARTITION BY MONTH(reading_read_at) ORDER BY reading_kwh DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_reading_voltage,
        LAG(reading_kwh, 1, 0) OVER w_reading_read_at_month_by_rea_2 AS prev_reading_kwh,
        LEAD(installed_kw) OVER (PARTITION BY reading_id ORDER BY reading_read_at, id, reading_id) AS next_installed_kw,
        ROUND(reading_kwh - AVG(reading_kwh) OVER (PARTITION BY reading_id), 2) AS reading_kwh_vs_reading_id_avg,
        FIRST_VALUE(reading_kwh) OVER (PARTITION BY MONTH(reading_read_at) ORDER BY reading_kwh DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reading_kwh,
        reading_kwh - LAG(reading_kwh) OVER (PARTITION BY MONTH(reading_read_at) ORDER BY reading_kwh DESC, id, reading_id) AS reading_kwh_change
    FROM profiled
    WINDOW w_reading_read_at_month_by_rea_2 AS (PARTITION BY MONTH(reading_read_at) ORDER BY reading_kwh DESC, id, reading_id)
)
SELECT
    id,
    installed_kw,
    reading_id,
    reading_quality,
    reading_kwh,
    avg_reading_voltage,
    prev_reading_kwh,
    next_installed_kw,
    reading_kwh_vs_reading_id_avg,
    first_reading_kwh,
    INITCAP(TRIM(reading_quality)) AS reading_quality_clean
FROM with_windows
ORDER BY reading_kwh, id NULLS LAST
