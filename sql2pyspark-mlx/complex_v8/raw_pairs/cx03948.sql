SELECT zone, commissioned_on, id
FROM sites
WHERE installed_kw <= 373.3
  AND LENGTH(TRIM(site_name)) >= 5
UNION
SELECT tariff, commissioned_on, id
FROM sites
WHERE installed_kw <= 284.0
  AND zone NOT IN ('Z2', 'Z3')
ORDER BY zone DESC, commissioned_on
