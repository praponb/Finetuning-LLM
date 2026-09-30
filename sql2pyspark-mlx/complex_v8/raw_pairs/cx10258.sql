WITH unioned AS (
    -- Stack the branches
    SELECT 'tariff' AS src, id, valid_to AS happened_at
    FROM tariff_windows
    WHERE valid_to >= TIMESTAMP '2025-01-25 21:00:00'
    UNION ALL
    SELECT 'reading' AS src, id, read_at AS happened_at
    FROM meter_readings
)
SELECT src, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(happened_at) AS latest_happened_at
FROM unioned
GROUP BY src
ORDER BY src
