SELECT
    city,
    country,
    supplier_name,
    TRIM(city) AS clean_city
FROM suppliers
