WITH combined AS (
    -- Stack the branches
    SELECT site_name, tariff AS label
    FROM sites
    UNION
    SELECT tariff AS site_name, site_name AS label
    FROM sites
)
SELECT site_name, COUNT(*) AS n_rows
FROM combined
GROUP BY site_name
ORDER BY site_name
