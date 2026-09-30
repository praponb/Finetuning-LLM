WITH base AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE tariff IN ('EV', 'FLAT')
)
SELECT zone AS category
FROM base
EXCEPT ALL
SELECT zone
FROM sites
ORDER BY category DESC
