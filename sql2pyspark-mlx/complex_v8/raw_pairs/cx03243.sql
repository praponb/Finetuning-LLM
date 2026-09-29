WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CONCAT(zone, '#', CAST(id AS STRING)) AS zone_ref
    FROM sites
    WHERE tariff IS NOT NULL
),
stacked AS (
    -- Stack the branches
    SELECT id AS qty, commissioned_on
    FROM base
    UNION ALL
    SELECT id AS qty, commissioned_on
    FROM sites
    WHERE commissioned_on < DATE '2024-11-21'
    UNION ALL
    SELECT id AS qty, commissioned_on
    FROM sites
    WHERE LOWER(site_name) LIKE '% n%'
      AND installed_kw < 446.9
)
SELECT qty, COUNT(*) AS n_rows, MAX(commissioned_on) AS latest_commissioned_on
FROM stacked
GROUP BY qty
ORDER BY qty
