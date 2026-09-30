WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        kwh - voltage AS kwh_minus_voltage
    FROM meter_readings
    WHERE kwh >= 37.0
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM cleaned s
),
profiled_2 AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_kwh
    FROM profiled s
)
SELECT *
FROM (
    SELECT site_id, read_at, quality, total_reading_kwh
    FROM profiled_2
) src
PIVOT (
    SUM(total_reading_kwh)
    FOR quality IN ('EST', 'BAD')
)
ORDER BY site_id, read_at
