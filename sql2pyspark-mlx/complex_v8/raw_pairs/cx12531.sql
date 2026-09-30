WITH base AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref
    FROM sites
    WHERE LENGTH(TRIM(site_name)) >= 8
      AND YEAR(commissioned_on) = 2024
)
SELECT id AS qty
FROM base
WHERE tariff NOT IN ('TOU', 'FLAT')
INTERSECT
SELECT id
FROM sites
ORDER BY qty DESC
