WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality <> 'BAD'
      AND site_id IS NOT NULL
),
combined_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        si.zone AS site_zone
    FROM base s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        LEAD(voltage) OVER (ORDER BY voltage DESC, id) AS next_voltage,
        voltage - LAG(voltage) OVER (ORDER BY voltage DESC, id) AS voltage_change,
        NTILE(5) OVER (ORDER BY voltage DESC, id) AS voltage_quintile,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality, site_id), 2) AS voltage_vs_quality_site_id_avg
    FROM combined_details_site
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        read_at + INTERVAL 1 DAYS AS read_at_plus_1d,
        GREATEST(voltage_quintile, next_voltage) AS greatest_voltage_quintile_next_voltage
    FROM metrics
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, voltage_vs_quality_site_id_avg, voltage, voltage_change, next_voltage
    FROM annotated
) src
UNPIVOT (
    amount FOR measure IN (voltage_vs_quality_site_id_avg AS voltage_vs_quality_site_id_avg_v, voltage AS voltage_v, voltage_change AS voltage_change_v, next_voltage AS m_next_voltage)
) unp
