SELECT country, SUM(rating) AS total_rating, RANK() OVER (ORDER BY SUM(rating) DESC) AS rnk FROM suppliers WHERE country = 'JP' GROUP BY country ORDER BY 2 DESC
