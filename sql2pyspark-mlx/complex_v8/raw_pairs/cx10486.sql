WITH stacked AS (
    -- Stack the branches
    SELECT id, kwh AS value
    FROM meter_readings
    UNION ALL
    SELECT site_id, kwh
    FROM meter_readings
    WHERE quality IN ('EST', 'OK')
)
SELECT id, COUNT(*) AS n_rows, SUM(value) AS total_value
FROM stacked
GROUP BY id
ORDER BY id
