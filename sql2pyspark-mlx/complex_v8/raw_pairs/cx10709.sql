WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 1.5, 2) AS voltage_adj,
        ROUND(voltage * 0.25, 2) AS voltage_adj_2,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE quality IN ('OK', 'EST')
      AND kwh < 81.1
),
rollup_base AS (
    -- Summarise per group
    SELECT
        TO_DATE(read_at) AS read_at_day,
        PERCENTILE_APPROX(voltage_adj_2, 0.5) AS median_voltage_adj_2,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        SUM(voltage) AS total_voltage
    FROM prepared_readings
    GROUP BY TO_DATE(read_at)
)
SELECT read_at_day, metric, metric_value
FROM (
    SELECT read_at_day, median_voltage_adj_2, total_voltage
    FROM rollup_base
) src
UNPIVOT (
    metric_value FOR metric IN (median_voltage_adj_2, total_voltage)
) unp
ORDER BY read_at_day, metric
