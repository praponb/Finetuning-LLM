WITH recent AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        installed_kw,
        commissioned_on,
        DATE_TRUNC('QUARTER', commissioned_on) AS commissioned_on_quarter
    FROM sites
)
SELECT id, installed_kw AS amount, commissioned_on
FROM recent
WHERE UPPER(TRIM(site_name)) LIKE 'R%'
UNION ALL
SELECT id, installed_kw AS amount, commissioned_on
FROM sites
ORDER BY id
