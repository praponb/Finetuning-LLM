WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh > 34.6
),
stats AS (
    -- Summarise per group
    SELECT
        site_id,
        ROUND(AVG(kwh), 2) AS avg_kwh,
        SUM(voltage) FILTER (WHERE quality = 'EST') AS voltage_quality_est,
        ROUND(100 * COUNT(CASE WHEN quality = 'OK' THEN 1 END) / COUNT(*), 1) AS pct_quality_ok,
        ROUND(AVG(voltage), 2) AS avg_voltage,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM valid
    GROUP BY site_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT site_id, metric_name, metric_value
FROM (
    SELECT site_id, pct_quality_ok, avg_kwh
    FROM stats
) src
UNPIVOT (
    metric_value FOR metric_name IN (pct_quality_ok, avg_kwh)
) unp
ORDER BY site_id, metric_name
