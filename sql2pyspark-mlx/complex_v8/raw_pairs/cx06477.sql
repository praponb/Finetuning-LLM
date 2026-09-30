WITH combined AS (
    -- Stack the branches
    SELECT site_name AS category, installed_kw AS metric, tariff
    FROM sites
    WHERE installed_kw >= 141.6
      AND UPPER(TRIM(site_name)) LIKE 'H%'
    UNION
    SELECT site_name, installed_kw, tariff
    FROM sites
    WHERE installed_kw < 284.0
)
SELECT category, COUNT(*) AS n_rows, SUM(metric) AS total_metric
FROM combined
GROUP BY category
ORDER BY category
