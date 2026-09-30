SELECT *, LOWER(city) AS city_lower FROM suppliers WHERE (country = 'UK' AND supplier_name IS NOT NULL) OR rating BETWEEN 2.0 AND 3.3 ORDER BY country
