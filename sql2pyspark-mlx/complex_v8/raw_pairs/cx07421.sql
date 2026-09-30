WITH unioned AS (
    -- Stack the branches
    SELECT zone AS category, tariff
    FROM sites
    WHERE UPPER(TRIM(site_name)) LIKE 'S%'
      AND installed_kw > 141.6
    UNION ALL
    SELECT site_name, zone
    FROM sites
)
SELECT category, COUNT(*) AS n_rows
FROM unioned
GROUP BY category
ORDER BY category
