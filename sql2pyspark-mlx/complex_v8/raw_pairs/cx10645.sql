WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        MONTH(read_at) AS read_at_month,
        CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE kwh > 14.4
      AND quality = 'EST'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, ROUND(AVG(kwh), 2) AS avg_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_kwh
    FROM cleaned s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
without_matches AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND UPPER(TRIM(si.site_name)) LIKE 'R%'
        )
),
quality_group_stats AS (
    -- Avg voltage per quality_group
    SELECT quality_group, AVG(voltage) AS avg_voltage
    FROM without_matches
    GROUP BY quality_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM without_matches s
    JOIN quality_group_stats g ON g.quality_group = s.quality_group
    WHERE s.voltage > g.avg_voltage
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    read_at_month,
    quality_group,
    avg_reading_kwh,
    avg_voltage,
    voltage_minus_avg,
    ROUND(avg_reading_kwh / NULLIF(read_at_month, 0), 3) AS avg_reading_kwh_per_read_at_month
FROM vs_group
WHERE kwh > 33.9
