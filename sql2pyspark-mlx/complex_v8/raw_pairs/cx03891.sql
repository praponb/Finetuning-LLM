WITH combined AS (
    -- Stack the branches
    SELECT
        'reading' AS record_type,
        site_id,
        read_at AS event_time,
        id AS qty
    FROM meter_readings
    WHERE kwh <= 79.4
    UNION ALL
    SELECT
        'reading_2' AS record_type,
        site_id,
        read_at AS event_time,
        id AS qty
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-02 13:00:00'
      AND voltage >= 221.4
)
SELECT record_type, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(event_time) AS latest_event_time
FROM combined
GROUP BY record_type
ORDER BY record_type
