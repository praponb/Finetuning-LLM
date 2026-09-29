SELECT city, MAX(sqft) - MIN(sqft) sqft_range, ROUND(AVG(sqft), 2) avg_sqft FROM stores WHERE opened_date <= '2025-06-01' GROUP BY city HAVING MAX(sqft) - MIN(sqft) > 62 ORDER BY avg_sqft;
