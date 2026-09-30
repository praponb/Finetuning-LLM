SELECT city, SUM(rating) total_rating, DENSE_RANK() OVER (ORDER BY SUM(rating) DESC) AS rnk FROM suppliers WHERE supplier_name IS NULL GROUP BY city ORDER BY city;
