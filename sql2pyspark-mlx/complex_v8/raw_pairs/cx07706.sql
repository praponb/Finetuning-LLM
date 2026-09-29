WITH filtered_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM sites
    WHERE zone IS NOT NULL
      AND installed_kw BETWEEN 93.3 AND 373.3
)
SELECT id AS entity_id
FROM filtered_sites
WHERE zone IN ('Z4', 'Z3')
MINUS
SELECT id
FROM sites
WHERE tariff <> 'TOU'
ORDER BY entity_id DESC
