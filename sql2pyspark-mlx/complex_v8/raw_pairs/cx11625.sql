WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.voltage) AS prior_voltage_12h
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.site_id, a.read_at
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE m.quality IS NOT NULL) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM joined_ranges s
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    prior_rows_12h,
    prior_voltage_12h
FROM profiled
ORDER BY prior_rows_12h DESC NULLS FIRST, id
LIMIT 10
