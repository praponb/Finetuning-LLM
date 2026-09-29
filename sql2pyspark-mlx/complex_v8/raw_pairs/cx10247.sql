WITH combined AS (
    -- Stack the branches
    SELECT tariff AS name, rate AS value
    FROM tariff_windows
    UNION ALL
    SELECT quality AS name, kwh AS value
    FROM meter_readings
)
SELECT name, COUNT(*) AS n_rows, SUM(value) AS total_value
FROM combined
GROUP BY name
ORDER BY name
