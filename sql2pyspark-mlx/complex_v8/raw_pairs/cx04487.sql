WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (quality <> 'OK' OR voltage BETWEEN 220.2 AND 237.5)
),
combined AS (
    -- Stack the branches
    SELECT site_id AS ref_id, read_at AS event_time, voltage AS metric
    FROM cleaned
    WHERE site_id IS NOT NULL
    UNION ALL
    SELECT id AS ref_id, read_at AS event_time, voltage AS metric
    FROM meter_readings
)
SELECT ref_id, COUNT(*) AS n_rows, SUM(metric) AS total_metric, MAX(event_time) AS latest_event_time
FROM combined
GROUP BY ref_id
ORDER BY ref_id
