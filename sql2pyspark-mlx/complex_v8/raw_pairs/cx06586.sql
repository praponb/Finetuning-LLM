WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
),
all_events AS (
    -- Stack the branches
    SELECT id, read_at, quality AS category
    FROM scoped_readings
    UNION ALL
    SELECT id, valid_to AS read_at, tariff AS category
    FROM tariff_windows
    WHERE valid_to < TIMESTAMP '2025-03-27 13:00:00'
)
SELECT category, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(read_at) AS latest_read_at
FROM all_events
GROUP BY category
ORDER BY category
