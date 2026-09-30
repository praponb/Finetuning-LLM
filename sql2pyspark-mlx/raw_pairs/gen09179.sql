SELECT city, SUM(rating) AS total_rating, SUM(SUM(rating)) OVER (ORDER BY city) AS cumulative_rating
FROM suppliers
WHERE supplier_name LIKE '%Part%'
GROUP BY city;
