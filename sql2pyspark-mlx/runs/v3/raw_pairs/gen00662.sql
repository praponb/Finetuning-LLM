SELECT *, REGEXP_REPLACE(supplier_name, ' ', '') AS supplier_name_slug, LOWER(country) AS country_lower
FROM suppliers
WHERE supplier_name IS NOT NULL OR supplier_name IS NOT NULL
ORDER BY country_lower
