WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        MONTH(read_at) AS read_at_month
    FROM meter_readings
    WHERE quality IN ('EST', 'OK')
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM cleaned_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage < 235.0
        )
      AND s.quality IS NOT NULL
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) FILTER (WHERE quality NOT IN ('BAD', 'OK')) AS reading_quality_count,
        COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM without_matches s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
SELECT id, metric_name, value
FROM (
    SELECT id, reading_count, read_at_month
    FROM with_stats
) src
UNPIVOT (
    value FOR metric_name IN (reading_count, read_at_month)
) unp
