WITH stacked AS (
    -- Stack the branches
    SELECT id, read_at AS event_time, quality
    FROM meter_readings
    UNION ALL
    SELECT id, read_at, quality
    FROM meter_readings
    WHERE voltage <= 237.5
)
SELECT quality, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(event_time) AS latest_event_time
FROM stacked
GROUP BY quality
ORDER BY quality
