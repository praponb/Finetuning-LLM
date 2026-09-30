SELECT
    id,
    country,
    city,
    TRIM(supplier_name) AS clean_supplier_name
FROM suppliers
WHERE country IN ('SG', 'UK', 'US') AND LENGTH(supplier_name) >= 14 AND rating < 4.0;
