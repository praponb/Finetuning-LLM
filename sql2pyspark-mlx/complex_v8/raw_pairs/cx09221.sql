WITH unioned AS (
    -- Stack the branches
    SELECT zone, installed_kw, tariff AS name
    FROM sites
    UNION ALL
    SELECT site_name AS zone, installed_kw, tariff AS name
    FROM sites
    WHERE UPPER(TRIM(site_name)) LIKE 'R%'
      AND installed_kw > 141.6
)
SELECT zone, COUNT(*) AS n_rows, SUM(installed_kw) AS total_installed_kw
FROM unioned
GROUP BY zone
ORDER BY zone
