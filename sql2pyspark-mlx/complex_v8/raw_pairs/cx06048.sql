WITH cleaned AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE installed_kw >= 141.6
      AND tariff IN ('FLAT', 'EV')
)
SELECT id AS entity_id
FROM cleaned
WHERE installed_kw < 373.3
  AND commissioned_on >= DATE '2024-08-24'
INTERSECT ALL
SELECT id AS entity_id
FROM sites
WHERE installed_kw >= 141.6
ORDER BY entity_id DESC
