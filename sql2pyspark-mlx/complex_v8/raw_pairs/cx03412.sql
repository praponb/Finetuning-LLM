WITH stacked AS (
    -- Stack the branches
    SELECT id AS key_id, site_id AS entity_id, kwh AS metric
    FROM meter_readings
    WHERE site_id <= 8
      AND read_at >= TIMESTAMP '2025-01-13 13:00:00'
    UNION ALL
    SELECT id, site_id, kwh
    FROM meter_readings
    UNION ALL
    SELECT id, site_id, voltage
    FROM meter_readings
)
SELECT key_id, COUNT(*) AS n_rows, SUM(metric) AS total_metric
FROM stacked
GROUP BY key_id
ORDER BY key_id
