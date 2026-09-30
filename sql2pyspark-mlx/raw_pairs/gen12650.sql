SELECT
    id,
    country,
    rating,
    CONCAT_WS(',', supplier_name, country) supplier_name_country_key
FROM suppliers
WHERE rating < 3.6
