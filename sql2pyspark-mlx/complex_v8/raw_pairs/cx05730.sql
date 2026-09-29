WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        HOUR(read_at) AS read_at_hour
    FROM meter_readings
    WHERE quality = 'EST'
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
    FROM cleaned s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        voltage,
        quality,
        read_at_hour,
        reading_count,
        ROUND(voltage * 1.5, 2) AS voltage_adj
    FROM summarized_rows
    WHERE site_id IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, voltage_adj, voltage
    FROM labeled
) src
UNPIVOT (
    amount FOR field IN (voltage_adj, voltage)
) unp
ORDER BY id, field
