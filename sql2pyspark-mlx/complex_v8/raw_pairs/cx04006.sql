WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh <= 79.4
),
combined AS (
    -- Stack the branches
    SELECT site_id, id, 'reading' AS origin
    FROM filtered
    WHERE site_id IS NOT NULL
    UNION ALL
    SELECT site_id, id, 'reading_2' AS origin
    FROM meter_readings
    UNION ALL
    SELECT id, site_id, 'reading_3' AS origin
    FROM meter_readings
    WHERE kwh < 111.9
)
SELECT origin, COUNT(*) AS n_rows, SUM(site_id) AS total_site_id
FROM combined
GROUP BY origin
ORDER BY origin
