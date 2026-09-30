WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM sites
    WHERE commissioned_on >= DATE '2024-08-24'
      AND tariff = 'EV'
)
SELECT 'site' AS origin, id, commissioned_on
FROM base
WHERE site_name IS NOT NULL
UNION
SELECT 'site_2' AS origin, id, commissioned_on
FROM sites
WHERE LOWER(site_name) LIKE '%ea%'
  AND installed_kw >= 141.6
ORDER BY origin, id
