WITH combined AS (
    -- Stack the branches
    SELECT id, rate AS metric, valid_to AS event_time
    FROM tariff_windows
    UNION ALL
    SELECT id, kwh, read_at
    FROM meter_readings
)
SELECT id, COUNT(*) AS n_rows, SUM(metric) AS total_metric, MAX(event_time) AS latest_event_time
FROM combined
GROUP BY id
ORDER BY id
