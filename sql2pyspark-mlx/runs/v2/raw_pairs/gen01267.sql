SELECT supplier_name, city, ROUND(rating * 1.07, 2) AS adjusted_rating
FROM suppliers
WHERE supplier_name NOT LIKE '%Supp%' OR country = 'JP'
ORDER BY supplier_name, adjusted_rating;
