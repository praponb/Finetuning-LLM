WITH recent_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        commissioned_on,
        CONCAT_WS('|', zone, tariff) AS zone_tariff_label
    FROM sites
    WHERE installed_kw >= 96.9
)
SELECT id AS key_id, installed_kw AS value, commissioned_on AS event_date
FROM sites
UNION
SELECT id AS key_id, installed_kw AS value, commissioned_on AS event_date
FROM sites
ORDER BY event_date, key_id
