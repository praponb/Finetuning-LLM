WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.voltage) AS prior_voltage_3d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT site_id, MAX(read_at) AS last_read_at
    FROM in_window
    GROUP BY site_id
    HAVING MAX(prior_rows_3d) > 2
)
SELECT site_id, TO_DATE(last_read_at) AS last_read_at_date
FROM stats
ORDER BY last_read_at_date DESC, site_id
