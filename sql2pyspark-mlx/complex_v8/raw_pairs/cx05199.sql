WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 101.2 THEN 'A' WHEN kwh >= 45.3 THEN 'B' ELSE 'C' END AS kwh_tier,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM meter_readings
),
quality_stats AS (
    -- Max voltage per quality
    SELECT quality, MAX(voltage) AS max_voltage
    FROM prepared_readings
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_voltage,
        ROUND(s.voltage - g.max_voltage, 2) AS voltage_minus_max
    FROM prepared_readings s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.voltage = g.max_voltage
)
SELECT id, measure, metric_value
FROM (
    SELECT id, voltage_minus_max, voltage, kwh, max_voltage
    FROM outliers
) src
UNPIVOT (
    metric_value FOR measure IN (voltage_minus_max, voltage, kwh, max_voltage)
) unp
