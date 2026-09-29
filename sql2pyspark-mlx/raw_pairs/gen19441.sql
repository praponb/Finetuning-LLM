SELECT
    id,
    rating,
    supplier_name,
    country,
    LENGTH(supplier_name) AS supplier_name_length
FROM suppliers
WHERE (city IN ('Tokyo', 'London', 'Berlin') AND city <> 'Berlin') OR rating <= 2.4
ORDER BY country ASC, id ASC
