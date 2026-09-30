WITH all_events AS (
    -- Stack the branches
    SELECT 'reading' AS record_type, quality, id AS entity_id
    FROM meter_readings
    WHERE quality = 'OK'
    UNION ALL
    SELECT 'tariff' AS record_type, tariff, id
    FROM tariff_windows
)
SELECT record_type, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM all_events
GROUP BY record_type
ORDER BY record_type
