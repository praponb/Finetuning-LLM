SELECT city, SUM(sqft) total_sqft, SUM(SUM(sqft)) OVER (ORDER BY city) AS cumulative_sqft FROM stores WHERE opened_date <= '2025-09-20' GROUP BY city;
