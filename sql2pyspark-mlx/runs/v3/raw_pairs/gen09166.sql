SELECT
    id,
    supplier_name,
    rating,
    city
FROM suppliers
WHERE city IN ('Singapore', 'New York', 'Tokyo') AND (country IN ('TH', 'JP') OR country <> 'UK')
ORDER BY supplier_name ASC
