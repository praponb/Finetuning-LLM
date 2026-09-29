WITH unioned AS (
    -- Stack the branches
    SELECT tariff, id, 'tariff' AS record_type
    FROM tariff_windows
    UNION ALL
    SELECT quality, id, 'reading' AS record_type
    FROM meter_readings
    WHERE site_id >= 3
)
SELECT record_type, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM unioned
GROUP BY record_type
ORDER BY record_type
