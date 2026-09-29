SELECT country, city, SUM(rating) AS total_rating FROM suppliers WHERE supplier_name LIKE '%EuroGoo%' AND country = 'TH' AND city IN ('New York', 'Bangkok') GROUP BY country, city
