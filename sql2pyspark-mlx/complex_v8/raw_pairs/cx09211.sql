WITH combined AS (
    -- Stack the branches
    SELECT
        site_id AS ref_id,
        id AS qty,
        voltage AS amount,
        'reading' AS origin
    FROM meter_readings
    UNION ALL
    SELECT
        id,
        site_id,
        kwh,
        'reading_2' AS origin
    FROM meter_readings
)
SELECT origin, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM combined
GROUP BY origin
ORDER BY origin
