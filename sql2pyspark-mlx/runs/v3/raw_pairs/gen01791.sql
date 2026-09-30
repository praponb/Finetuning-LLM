SELECT supplier_name, rating, city
FROM suppliers
WHERE LOWER(supplier_name) LIKE '%aci%' AND country IN ('TH', 'US');
