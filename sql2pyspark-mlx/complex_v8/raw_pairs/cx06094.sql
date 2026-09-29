WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 09:00:00'
),
quality_stats AS (
    -- Max voltage per quality
    SELECT quality, MAX(voltage) AS max_voltage
    FROM valid
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_voltage,
        ROUND(s.voltage - g.max_voltage, 2) AS voltage_minus_max
    FROM valid s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.voltage = g.max_voltage
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(voltage_minus_max, max_voltage) AS least_voltage_minus_max_max_voltage
    FROM outliers
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.max_voltage,
        s.voltage_minus_max,
        s.least_voltage_minus_max_max_voltage,
        si.commissioned_on AS site_commissioned_on
    FROM tagged s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on >= DATE '2024-10-28'
)
SELECT id, measure, amount
FROM (
    SELECT id, least_voltage_minus_max_max_voltage, voltage, max_voltage, kwh
    FROM combined_details
) src
UNPIVOT (
    amount FOR measure IN (least_voltage_minus_max_max_voltage, voltage, max_voltage, kwh)
) unp
ORDER BY id, measure
