WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LEAST(kwh, voltage) AS least_kwh_voltage
    FROM meter_readings
),
unioned AS (
    -- Stack the branches
    SELECT
        'reading' AS src,
        id,
        site_id AS key_id,
        read_at AS happened_at
    FROM meter_readings
    UNION ALL
    SELECT
        'reading_2' AS src,
        site_id AS id,
        id AS key_id,
        read_at AS happened_at
    FROM meter_readings
)
SELECT src, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(happened_at) AS latest_happened_at
FROM unioned
GROUP BY src
ORDER BY src
