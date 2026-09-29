SELECT id, supplier_name, COALESCE(city, 'Unknown') AS city_filled
FROM suppliers
