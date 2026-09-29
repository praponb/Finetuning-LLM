WITH valid AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE tariff NOT IN ('TOU', 'EV')
)
SELECT
    'site' AS src,
    id AS entity_id,
    installed_kw,
    commissioned_on
FROM valid
WHERE commissioned_on < DATE '2025-05-10'
  AND installed_kw >= 96.9
UNION ALL
SELECT
    'site_2' AS src,
    id,
    installed_kw,
    commissioned_on
FROM sites
WHERE zone = 'Z4'
UNION ALL
SELECT
    'site_3' AS src,
    id,
    installed_kw,
    commissioned_on
FROM sites
WHERE tariff IN ('TOU', 'FLAT')
  AND zone = 'Z4'
ORDER BY entity_id
