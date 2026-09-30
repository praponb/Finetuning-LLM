WITH all_events AS (
    -- Stack the branches
    SELECT
        'site' AS src,
        zone,
        installed_kw AS metric,
        id AS ref_id
    FROM sites
    UNION ALL
    SELECT
        'reading' AS src,
        quality,
        kwh,
        id
    FROM meter_readings
    WHERE voltage BETWEEN 215.1 AND 239.6
      AND site_id IS NOT NULL
)
SELECT src, COUNT(*) AS n_rows, SUM(metric) AS total_metric
FROM all_events
GROUP BY src
ORDER BY src
