WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage < 233.2
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality_clean,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM recent_readings s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage <= 233.6
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at
    FROM detail_rows_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
combined_details_reading AS (
    -- Join meter_readings
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.reading_id,
        s.reading_quality,
        s.reading_voltage,
        s.first_reading_read_at,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality_2
    FROM summarized_rows s
    JOIN meter_readings m ON m.id = s.reading_id
    WHERE HOUR(m.read_at) BETWEEN 8 AND 12
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT reading_quality, site_id, reading_quality_2, reading_kwh
    FROM combined_details_reading
) src
PIVOT (
    SUM(reading_kwh)
    FOR reading_quality_2 IN ('BAD', 'EST', 'OK')
)
ORDER BY reading_quality, site_id
