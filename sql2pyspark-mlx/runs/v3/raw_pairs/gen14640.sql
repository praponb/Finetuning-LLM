SELECT *, TRIM(country) AS clean_country
FROM suppliers
WHERE country = 'US' AND rating > 4.1
ORDER BY supplier_name;
