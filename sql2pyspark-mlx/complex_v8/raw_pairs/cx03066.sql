WITH recent AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE site_name IS NOT NULL
      AND zone <> 'Z4'
)
SELECT id
FROM recent
WHERE UPPER(TRIM(site_name)) LIKE 'R%'
EXCEPT
SELECT site_id
FROM meter_readings
