WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 2)
      AND quality IN ('OK', 'BAD')
),
quality_stats AS (
    -- Avg voltage per quality
    SELECT quality, AVG(voltage) AS avg_voltage
    FROM recent
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM recent s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.voltage > g.avg_voltage
),
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.kwh_filled,
        s.avg_voltage,
        s.voltage_minus_avg,
        si.zone AS site_zone,
        si.site_name
    FROM above_average s
    JOIN sites si ON si.id = s.site_id
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh <= 81.3
              AND HOUR(m.read_at) BETWEEN 6 AND 8
        )
),
site_id_stats AS (
    -- Avg voltage per site_id
    SELECT site_id, AVG(voltage) AS avg_voltage_2
    FROM screened
    GROUP BY site_id
),
above_average_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage_2,
        ROUND(s.voltage - g.avg_voltage_2, 2) AS voltage_minus_avg_2
    FROM screened s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage > g.avg_voltage_2
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.voltage_minus_avg,
        s.site_zone,
        s.site_name,
        s.avg_voltage_2,
        s.voltage_minus_avg_2,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone_2,
        si.site_name AS site_site_name
    FROM above_average_2 s
    JOIN sites si ON si.id = s.site_id
)
SELECT id, field, value
FROM (
    SELECT id, voltage_minus_avg, voltage_minus_avg_2, voltage, avg_voltage_2
    FROM enriched
) src
UNPIVOT (
    value FOR field IN (voltage_minus_avg, voltage_minus_avg_2, voltage, avg_voltage_2)
) unp
