WITH recent_sites AS (
    -- Filter sites
    SELECT
        id,
        zone,
        tariff,
        commissioned_on,
        CONCAT(site_name, '#', CAST(id AS STRING)) AS site_name_ref
    FROM sites
    WHERE LENGTH(TRIM(site_name)) >= 8
      AND installed_kw <= 446.9
)
SELECT
    tariff,
    commissioned_on,
    zone AS label,
    'site' AS source
FROM recent_sites
UNION ALL
SELECT
    tariff,
    commissioned_on,
    zone AS label,
    'site_2' AS source
FROM sites
WHERE LENGTH(TRIM(site_name)) >= 6
ORDER BY commissioned_on DESC, source
