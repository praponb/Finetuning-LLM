SELECT
    id,
    rating,
    country,
    TRIM(supplier_name) AS clean_supplier_name
FROM suppliers
WHERE country = 'US'
