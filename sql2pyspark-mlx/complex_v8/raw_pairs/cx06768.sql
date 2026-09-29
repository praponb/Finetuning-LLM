WITH stacked AS (
    -- Stack the branches
    SELECT
        site_id,
        read_at,
        voltage AS value,
        'reading' AS src
    FROM meter_readings
    UNION ALL
    SELECT
        id,
        read_at,
        kwh,
        'reading_2' AS src
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage < 234.5
)
SELECT src, COUNT(*) AS n_rows, SUM(value) AS total_value, MAX(read_at) AS latest_read_at
FROM stacked
GROUP BY src
ORDER BY src
