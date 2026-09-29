WITH stacked AS (
    -- Stack the branches
    SELECT id, site_id, 'reading' AS src
    FROM meter_readings
    WHERE voltage > 223.0
    UNION ALL
    SELECT site_id, id, 'reading_2' AS src
    FROM meter_readings
    WHERE voltage >= 224.8
      AND read_at >= TIMESTAMP '2025-01-25 11:00:00'
)
SELECT src, COUNT(*) AS n_rows, SUM(site_id) AS total_site_id
FROM stacked
GROUP BY src
ORDER BY src
