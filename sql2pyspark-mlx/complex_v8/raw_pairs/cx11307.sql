WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CASE WHEN voltage >= 238.8 THEN 'large' WHEN voltage >= 218.2 THEN 'medium' ELSE 'small' END AS voltage_tier,
        COALESCE(kwh, 0.0) AS kwh_filled,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM meter_readings
),
voltage_tier_stats AS (
    -- Avg kwh per voltage_tier
    SELECT voltage_tier, AVG(kwh) AS avg_kwh
    FROM cleaned_readings
    GROUP BY voltage_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM cleaned_readings s
    JOIN voltage_tier_stats g ON g.voltage_tier = s.voltage_tier
    WHERE s.kwh > g.avg_kwh
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.kwh_filled,
        s.least_voltage_kwh,
        s.avg_kwh,
        s.kwh_minus_avg,
        si.tariff AS site_tariff
    FROM compared s
    JOIN sites si ON si.id = s.site_id
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM combined_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 87.1
              AND m.read_at < TIMESTAMP '2025-03-02 09:00:00'
        )
      AND s.site_tariff = 'FLAT'
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    least_voltage_kwh,
    avg_kwh,
    kwh_minus_avg,
    site_tariff
FROM filtered
WHERE voltage >= 224.8
ORDER BY id, site_tariff DESC
