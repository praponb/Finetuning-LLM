SELECT city, SUM(rating) AS total_rating, COUNT(*) AS cnt, SUM(SUM(rating)) OVER (ORDER BY city) AS cumulative_rating FROM suppliers GROUP BY city
