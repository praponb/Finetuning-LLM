WITH cleaned_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE LOWER(site_name) LIKE '%rm%'
      AND tariff <> 'EV'
)
SELECT id, installed_kw, commissioned_on
FROM cleaned_sites
WHERE commissioned_on < DATE '2024-11-21'
UNION
SELECT id, installed_kw, commissioned_on
FROM sites
ORDER BY id
