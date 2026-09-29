SELECT
    id,
    rating,
    supplier_name,
    TRIM(supplier_name) AS clean_supplier_name,
    FLOOR(rating) AS rating_whole
FROM suppliers
WHERE country = 'US' AND supplier_name LIKE '%Globa%' AND LENGTH(supplier_name) < 12
