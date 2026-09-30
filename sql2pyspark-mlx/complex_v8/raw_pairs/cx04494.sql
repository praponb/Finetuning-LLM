WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean,
        ROUND(voltage * 1.5, 2) AS voltage_adj,
        CASE WHEN kwh >= 89.8 THEN 'A' WHEN kwh >= 51.5 THEN 'B' ELSE 'C' END AS kwh_tier
    FROM meter_readings
    WHERE quality = 'BAD'
),
kwh_tier_stats AS (
    -- Avg voltage per kwh_tier
    SELECT kwh_tier, AVG(voltage) AS avg_voltage
    FROM valid_readings
    GROUP BY kwh_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM valid_readings s
    JOIN kwh_tier_stats g ON g.kwh_tier = s.kwh_tier
    WHERE s.voltage > g.avg_voltage
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, voltage_adj, avg_voltage, voltage, voltage_minus_avg
    FROM compared
) src
UNPIVOT (
    metric_value FOR metric IN (voltage_adj, avg_voltage, voltage, voltage_minus_avg)
) unp
