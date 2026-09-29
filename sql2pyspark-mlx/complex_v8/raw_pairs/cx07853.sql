WITH cleaned AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE commissioned_on BETWEEN DATE '2024-08-24' AND DATE '2025-12-03'
)
SELECT id, zone AS name
FROM cleaned
INTERSECT
SELECT id, zone AS name
FROM sites
ORDER BY name DESC, id
