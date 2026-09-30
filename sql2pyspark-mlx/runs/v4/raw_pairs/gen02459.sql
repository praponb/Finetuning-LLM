SELECT
    id,
    supplier_name,
    city,
    LENGTH(country) AS country_length
FROM suppliers
ORDER BY country_length, supplier_name;
