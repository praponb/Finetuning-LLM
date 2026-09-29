WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality <> 'OK' THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count
    FROM filtered s
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_stats
    WHERE voltage > (SELECT AVG(voltage) FROM with_stats)
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    reading_quality_count
FROM qualified
ORDER BY id, read_at DESC NULLS FIRST
LIMIT 5
