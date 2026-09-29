SELECT city, SUM(rating) AS total_rating, SUM(SUM(rating)) OVER (ORDER BY city) AS cumulative_rating
FROM suppliers
GROUP BY city
ORDER BY city DESC
