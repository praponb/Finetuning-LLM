SELECT
    id,
    city,
    country,
    rating,
    LOWER(supplier_name) AS supplier_name_lower
FROM suppliers;
