WITH cleaned AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE YEAR(commissioned_on) = 2024
)
SELECT 'site' AS record_type, id, commissioned_on
FROM cleaned
UNION ALL
SELECT 'site_2' AS record_type, id, commissioned_on
FROM sites
ORDER BY record_type DESC
