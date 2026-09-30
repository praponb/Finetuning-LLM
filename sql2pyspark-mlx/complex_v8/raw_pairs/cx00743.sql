WITH scoped_sites AS (
    -- Scope the sites rows
    SELECT *
    FROM sites
    WHERE YEAR(commissioned_on) = 2025
      AND UPPER(TRIM(site_name)) LIKE 'D%'
),
stacked AS (
    -- Stack the branches
    SELECT id, tariff AS category, commissioned_on AS day
    FROM scoped_sites
    UNION ALL
    SELECT id, tariff AS category, commissioned_on AS day
    FROM sites
    WHERE tariff <> 'FLAT'
)
SELECT category, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(day) AS latest_day
FROM stacked
GROUP BY category
ORDER BY category
