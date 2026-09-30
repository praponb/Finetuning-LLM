SELECT country, SUM(rating) AS total_rating, ROUND(SUM(rating) * 100.0 / SUM(SUM(rating)) OVER (), 2) AS pct_of_total FROM suppliers GROUP BY country ORDER BY pct_of_total DESC, total_rating
