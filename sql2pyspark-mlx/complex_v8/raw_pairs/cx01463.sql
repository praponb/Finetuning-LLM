WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh < 84.9
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, SUM(voltage) AS total_reading_voltage, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.total_reading_voltage,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM cleaned s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM profiled
    WHERE total_reading_voltage < (SELECT MAX(total_reading_voltage) FROM profiled)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        voltage,
        total_reading_voltage,
        reading_count,
        LEAST(total_reading_voltage, kwh) AS least_total_reading_voltage_kwh
    FROM qualified
    WHERE voltage < 234.5
)
SELECT
    id,
    voltage,
    total_reading_voltage,
    reading_count,
    least_total_reading_voltage_kwh,
    CASE WHEN reading_count >= 5 THEN 'large' WHEN reading_count >= 3 THEN 'medium' ELSE 'small' END AS reading_count_tier
FROM annotated
WHERE least_total_reading_voltage_kwh >= 220.5
