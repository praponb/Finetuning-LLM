SELECT city, SUM(sqft) AS total_sqft, COUNT(*) AS cnt, SUM(SUM(sqft)) OVER (ORDER BY city) AS cumulative_sqft FROM stores WHERE opened_date < '2025-05-16' GROUP BY city ORDER BY 4 DESC, 1 ASC
