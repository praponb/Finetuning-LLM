SELECT country, SUM(rating) AS total_rating, RANK() OVER (ORDER BY SUM(rating) DESC) AS rnk
FROM suppliers
WHERE supplier_name NOT LIKE '%EuroGoo%'
GROUP BY country
ORDER BY rnk ASC;
