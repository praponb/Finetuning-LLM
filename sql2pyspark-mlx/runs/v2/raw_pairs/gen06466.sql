SELECT city, SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END) AS berlin_count
FROM suppliers
WHERE rating > 4.1 AND (city = 'New York' OR LENGTH(supplier_name) > 5)
GROUP BY 1
HAVING berlin_count > 49
ORDER BY berlin_count DESC, city
LIMIT 5
