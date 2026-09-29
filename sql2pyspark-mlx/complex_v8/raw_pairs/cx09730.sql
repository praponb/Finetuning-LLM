WITH stacked AS (
    -- Stack the branches
    SELECT quality AS category, read_at AS happened_at, 'reading' AS src
    FROM meter_readings
    UNION ALL
    SELECT quality, read_at, 'reading_2' AS src
    FROM meter_readings
    UNION ALL
    SELECT tariff, valid_from, 'tariff' AS src
    FROM tariff_windows
    WHERE rate <= 5.5
      AND tariff IN ('EV', 'TOU')
)
SELECT src, COUNT(*) AS n_rows, MAX(happened_at) AS latest_happened_at
FROM stacked
GROUP BY src
ORDER BY src
