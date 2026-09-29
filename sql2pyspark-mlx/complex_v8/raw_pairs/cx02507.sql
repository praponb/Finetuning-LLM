WITH all_events AS (
    -- Stack the branches
    SELECT 'reading' AS src, id, read_at
    FROM meter_readings
    UNION
    SELECT 'tariff' AS src, id, valid_from
    FROM tariff_windows
    WHERE valid_to >= TIMESTAMP '2025-02-15 04:00:00'
)
SELECT src, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(read_at) AS latest_read_at
FROM all_events
GROUP BY src
ORDER BY src
