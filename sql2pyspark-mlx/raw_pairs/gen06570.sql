SELECT
    country,
    supplier_name,
    city,
    CONCAT_WS('-', supplier_name, country) AS supplier_name_country_key
FROM suppliers
WHERE (country NOT IN ('DE', 'UK', 'US') AND city IS NULL) OR supplier_name LIKE '%Whole%'
ORDER BY supplier_name DESC;
