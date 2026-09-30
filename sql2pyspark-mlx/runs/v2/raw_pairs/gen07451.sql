SELECT
    supplier_name,
    city,
    country,
    CONCAT_WS('-', supplier_name, city) AS supplier_name_city_key,
    TRIM(city) AS clean_city
FROM suppliers
WHERE country <> 'TH'
