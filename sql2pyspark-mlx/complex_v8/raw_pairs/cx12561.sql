WITH base_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        tariff,
        installed_kw,
        commissioned_on,
        DATE_ADD(commissioned_on, 30) AS commissioned_on_plus_30d
    FROM sites
)
SELECT tariff AS category
FROM base_sites
EXCEPT
SELECT tariff
FROM sites
