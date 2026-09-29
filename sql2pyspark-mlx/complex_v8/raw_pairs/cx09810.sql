WITH filtered_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE installed_kw BETWEEN 96.9 AND 446.9
      AND zone IN ('Z4', 'Z3')
)
SELECT id
FROM filtered_sites
INTERSECT
SELECT id
FROM tariff_windows
WHERE MONTH(valid_from) IN (1, 3)
ORDER BY id
