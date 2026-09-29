SELECT id, supplier_name, LENGTH(city) city_length
FROM suppliers
WHERE (rating >= 3.7 AND city = 'Bangkok') OR city = 'New York'
