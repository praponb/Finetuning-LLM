SELECT *, CASE WHEN country = 'UK' THEN 1 ELSE 0 END AS is_uk, CONCAT(supplier_name, ', ', country) AS supplier_name_country
FROM suppliers
WHERE NOT (city IS NOT NULL AND city != 'Tokyo')
