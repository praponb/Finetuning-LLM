WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id >= 9
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality = 'BAD' THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count
    FROM base s
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_kwh,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT MIN(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_voltage
    FROM summarized_rows s
)
SELECT
    reading_quality_count,
    quality,
    site_id,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(min_reading_voltage), 2) AS stddev_min_reading_voltage
FROM with_counts
GROUP BY CUBE(reading_quality_count, quality, site_id)
ORDER BY reading_quality_count NULLS LAST, quality NULLS LAST, site_id NULLS LAST
