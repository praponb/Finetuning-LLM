WITH base_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        UPPER(TRIM(zone)) AS zone_clean
    FROM sites
    WHERE commissioned_on < DATE '2025-05-10'
),
all_events AS (
    -- Stack the branches
    SELECT site_name, zone
    FROM base_sites
    WHERE LENGTH(TRIM(zone_clean)) >= 6
      AND UPPER(TRIM(site_name)) LIKE 'H%'
    UNION
    SELECT tariff, zone
    FROM sites
)
SELECT site_name, COUNT(*) AS n_rows
FROM all_events
GROUP BY site_name
ORDER BY site_name
