WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled,
        TO_DATE(read_at) AS read_at_date,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
    WHERE kwh BETWEEN 16.5 AND 90.6
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(m.voltage), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_voltage
    FROM prepared_readings s
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.kwh_filled,
        s.read_at_date,
        s.read_at_year,
        s.avg_reading_voltage,
        si.zone AS site_zone,
        si.site_name
    FROM summarized_rows s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    site_id,
    read_at_year,
    quality,
    GROUPING_ID() AS grouping_level,
    ROUND(AVG(avg_reading_voltage), 2) AS avg_avg_reading_voltage
FROM with_details_site
GROUP BY GROUPING SETS ((site_id, read_at_year, quality), (site_id), (read_at_year))
ORDER BY site_id NULLS LAST, read_at_year NULLS LAST, quality NULLS LAST
