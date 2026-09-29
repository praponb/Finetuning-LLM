WITH combined AS (
    -- Stack the branches
    SELECT site_id AS key_id, read_at AS happened_at, quality
    FROM meter_readings
    WHERE kwh < 109.0
      AND quality IN ('BAD', 'OK')
    UNION ALL
    SELECT id AS key_id, read_at AS happened_at, quality
    FROM meter_readings
)
SELECT quality, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id, MAX(happened_at) AS latest_happened_at
FROM combined
GROUP BY quality
ORDER BY quality
