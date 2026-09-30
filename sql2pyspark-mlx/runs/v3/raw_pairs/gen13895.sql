SELECT
    id,
    supplier_name,
    city,
    CONCAT(supplier_name, '/', city) supplier_name_city
FROM suppliers
WHERE rating <= 3.7 AND country IN ('UK', 'TH') AND rating >= 1.9
ORDER BY supplier_name;
