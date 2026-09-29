WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label,
        ABS(voltage - 227.6) AS voltage_distance
    FROM meter_readings
    WHERE voltage <= 233.1
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM filtered s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, voltage_distance, voltage, kwh
    FROM summarized_rows
) src
UNPIVOT (
    metric_value FOR measure IN (voltage_distance, voltage, kwh)
) unp
ORDER BY id, measure
