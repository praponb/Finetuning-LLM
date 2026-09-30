SELECT city, SUM(rating) AS total_rating, ROUND(SUM(rating) * 100.0 / SUM(SUM(rating)) OVER (), 2) AS pct_of_total FROM suppliers GROUP BY city ORDER BY total_rating DESC;
