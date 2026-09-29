WITH recent AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE zone = 'Z2'
      AND LENGTH(TRIM(site_name)) >= 4
),
stacked AS (
    -- Stack the branches
    SELECT zone, commissioned_on AS day, 'site' AS record_type
    FROM recent
    UNION ALL
    SELECT site_name AS zone, commissioned_on AS day, 'site_2' AS record_type
    FROM sites
    WHERE commissioned_on < DATE '2025-05-07'
)
SELECT record_type, COUNT(*) AS n_rows, MAX(day) AS latest_day
FROM stacked
GROUP BY record_type
ORDER BY record_type
