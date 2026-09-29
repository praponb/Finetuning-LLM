WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 0.9, 2) AS kwh_adj,
        MONTH(read_at) AS read_at_month
    FROM meter_readings
    WHERE (quality = 'BAD' OR voltage > 221.8)
),
quality_stats AS (
    -- Max voltage per quality
    SELECT quality, MAX(voltage) AS max_voltage
    FROM prepared
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_voltage,
        ROUND(s.voltage - g.max_voltage, 2) AS voltage_minus_max
    FROM prepared s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.voltage = g.max_voltage
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM above_average s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_voltage,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM screened s
)
SELECT
    id,
    quality,
    read_at_month,
    max_voltage,
    total_reading_voltage,
    last_reading_read_at
FROM with_counts
