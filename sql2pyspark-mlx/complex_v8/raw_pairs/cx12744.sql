WITH filtered AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
    FROM sites
    WHERE commissioned_on IS NOT NULL
)
SELECT id AS entity_id, commissioned_on
FROM filtered
WHERE installed_kw <= 386.6
UNION ALL
SELECT id AS entity_id, commissioned_on
FROM sites
WHERE zone <> 'Z2'
  AND site_name IS NOT NULL
