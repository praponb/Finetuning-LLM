WITH combined AS (
    -- Stack the branches
    SELECT site_id, read_at AS event_time, 'reading' AS record_type
    FROM meter_readings
    UNION ALL
    SELECT site_id, read_at, 'reading_2' AS record_type
    FROM meter_readings
    UNION ALL
    SELECT site_id, read_at, 'reading_3' AS record_type
    FROM meter_readings
)
SELECT record_type, COUNT(*) AS n_rows, SUM(site_id) AS total_site_id, MAX(event_time) AS latest_event_time
FROM combined
GROUP BY record_type
ORDER BY record_type
