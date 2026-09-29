WITH unioned AS (
    -- Stack the branches
    SELECT site_id, voltage AS metric, 'reading' AS origin
    FROM meter_readings
    WHERE voltage < 231.6
    UNION
    SELECT id, rate, 'tariff' AS origin
    FROM tariff_windows
)
SELECT origin, COUNT(*) AS n_rows, SUM(metric) AS total_metric
FROM unioned
GROUP BY origin
ORDER BY origin
