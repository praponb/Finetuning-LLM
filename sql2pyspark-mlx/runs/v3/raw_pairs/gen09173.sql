SELECT
    city,
    supplier_name,
    rating,
    UPPER(supplier_name) AS supplier_name_upper
FROM suppliers
WHERE supplier_name LIKE '%Trader%' AND country NOT IN ('DE', 'UK', 'SG') AND rating < 2.2;
