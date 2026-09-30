SELECT
    id,
    supplier_name,
    rating,
    country,
    CONCAT(supplier_name, '/', city) AS supplier_name_city
FROM suppliers
