WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CONCAT_WS(': ', tariff, zone) AS tariff_zone_label
    FROM sites
    WHERE zone IN ('Z4', 'Z2')
)
SELECT id AS entity_id
FROM cleaned
WHERE tariff = 'TOU'
MINUS
SELECT id
FROM sites
