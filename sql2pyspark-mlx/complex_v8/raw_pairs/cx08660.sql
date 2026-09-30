WITH stacked AS (
    -- Stack the branches
    SELECT id, read_at
    FROM meter_readings
    WHERE voltage > 216.8
      AND kwh > 48.2
    UNION
    SELECT site_id, read_at
    FROM meter_readings
)
SELECT id, COUNT(*) AS n_rows, MAX(read_at) AS latest_read_at
FROM stacked
GROUP BY id
ORDER BY id
