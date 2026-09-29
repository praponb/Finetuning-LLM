WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at + INTERVAL 30 MINUTES AS read_at_plus_30m,
        CASE WHEN kwh >= 110.5 THEN 'HIGH' WHEN kwh >= 29.2 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM meter_readings
    WHERE site_id <= 7
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(read_at_dayofweek) OVER (PARTITION BY read_at_dayofweek, TO_DATE(read_at)) AS read_at_dayofweek_read_at_to_date_sum_read_at_da,
        ROUND(AVG(read_at_dayofweek) OVER (PARTITION BY read_at_dayofweek, TO_DATE(read_at)), 2) AS read_at_dayofweek_read_at_to_date_avg_read_at_da
    FROM cleaned
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.read_at_plus_30m,
        s.kwh_tier,
        si.installed_kw AS site_installed_kw
    FROM ranked s
    JOIN sites si ON si.id = s.site_id
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.read_at_plus_30m,
        s.kwh_tier,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at
    FROM with_details_site s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT id, reading_id, metric_name, reading
FROM (
    SELECT id, reading_id, reading_kwh, reading_voltage
    FROM with_children
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (reading_kwh, reading_voltage)
) unp
