SELECT
    id,
    supplier_name,
    CONCAT_WS('/', supplier_name, city) AS supplier_name_city_key,
    COALESCE(city, 'unassigned') AS city_filled
FROM suppliers
WHERE (supplier_name LIKE 'G%' AND rating > 2.5) OR country NOT IN ('DE', 'UK', 'TH')
