SELECT
    rating,
    supplier_name,
    city,
    LOWER(supplier_name) supplier_name_lower
FROM suppliers
WHERE supplier_name LIKE 'To%' AND rating BETWEEN 3.1 AND 4.0
