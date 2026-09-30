SELECT id, name, COALESCE(name, 'Unknown') name_filled, CONCAT(city, ' - ', segment) city_segment FROM customers WHERE country = 'JP' AND email IS NOT NULL ORDER BY id DESC
