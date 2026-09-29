WITH combined AS (
    -- Stack the branches
    SELECT site_id, quality, read_at AS happened_at
    FROM meter_readings
    WHERE quality IS NOT NULL
      AND voltage >= 223.0
    UNION ALL
    SELECT id, tariff, valid_to
    FROM tariff_windows
)
SELECT quality, COUNT(*) AS n_rows, SUM(site_id) AS total_site_id, MAX(happened_at) AS latest_happened_at
FROM combined
GROUP BY quality
ORDER BY quality
