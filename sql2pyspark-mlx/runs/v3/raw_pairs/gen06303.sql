SELECT country, MAX(rating) AS max_rating
FROM suppliers
WHERE UPPER(supplier_name) LIKE '%ION%' OR country = 'TH'
GROUP BY country;
