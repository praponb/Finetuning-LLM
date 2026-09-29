WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        INITCAP(TRIM(quality)) AS quality_clean,
        CASE WHEN voltage >= 237.8 THEN 'A' WHEN voltage >= 222.6 THEN 'B' ELSE 'C' END AS voltage_tier,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE site_id <= 3
      AND quality IN ('EST', 'BAD')
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM scoped_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IS NOT NULL
              AND m.read_at < TIMESTAMP '2025-02-12 08:00:00'
        )
),
voltage_tier_stats AS (
    -- Avg voltage per voltage_tier
    SELECT voltage_tier, AVG(voltage) AS avg_voltage
    FROM filtered
    GROUP BY voltage_tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM filtered s
    JOIN voltage_tier_stats g ON g.voltage_tier = s.voltage_tier
    WHERE s.voltage > g.avg_voltage
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality_clean,
        voltage_tier,
        quality_len,
        voltage_minus_avg,
        COUNT(*) OVER w_voltage_tier AS rows_in_voltage_tier,
        SUM(kwh) OVER (PARTITION BY voltage_tier) AS voltage_tier_sum_kwh,
        avg_voltage - LAG(avg_voltage) OVER (ORDER BY voltage ASC, id) AS avg_voltage_change,
        NTH_VALUE(kwh, 2) OVER (ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        MAX(voltage) OVER (PARTITION BY voltage_tier) AS voltage_tier_max_voltage
    FROM vs_group
    WINDOW w_voltage_tier AS (PARTITION BY voltage_tier)
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality_clean,
    voltage_tier,
    quality_len,
    voltage_minus_avg,
    rows_in_voltage_tier,
    voltage_tier_sum_kwh,
    second_kwh
FROM metrics
WHERE avg_voltage_change > -49.0
ORDER BY voltage_minus_avg
