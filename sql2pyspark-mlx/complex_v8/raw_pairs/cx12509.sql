WITH cleaned AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        LENGTH(TRIM(tariff)) AS tariff_len
    FROM sites
)
SELECT id AS key_id, commissioned_on
FROM sites
UNION ALL
SELECT id AS key_id, commissioned_on
FROM sites
WHERE LENGTH(TRIM(site_name)) >= 6
ORDER BY commissioned_on DESC
