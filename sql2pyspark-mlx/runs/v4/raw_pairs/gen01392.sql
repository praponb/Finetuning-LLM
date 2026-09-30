SELECT country, rating, CASE WHEN country = 'DE' THEN 1 ELSE 0 END is_de
FROM suppliers
WHERE country IN ('DE', 'US', 'UK')
