SELECT store_id, ROUND(AVG(revenue), 2) avg_revenue, MAX(units) - MIN(units) units_range FROM store_sales GROUP BY store_id HAVING avg_revenue >= 42
