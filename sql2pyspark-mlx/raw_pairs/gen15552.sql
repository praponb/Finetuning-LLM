SELECT *, INITCAP(TRIM(supplier_name)) AS supplier_name_title, CONCAT_WS('-', city, country) AS city_country_key
FROM suppliers;
