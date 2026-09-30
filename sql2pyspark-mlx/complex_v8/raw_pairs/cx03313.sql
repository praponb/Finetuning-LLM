WITH prepared_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        installed_kw
    FROM sites
    WHERE commissioned_on >= DATE '2024-10-28'
)
SELECT id AS key_id, installed_kw
FROM prepared_sites
WHERE UPPER(TRIM(site_name)) LIKE 'S%'
  AND zone NOT IN ('Z2', 'Z4')
UNION ALL
SELECT site_id AS key_id, voltage AS installed_kw
FROM meter_readings
WHERE voltage > 215.6
UNION ALL
SELECT id AS key_id, installed_kw
FROM sites
WHERE site_name IS NOT NULL
