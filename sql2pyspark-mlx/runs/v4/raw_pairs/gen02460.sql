SELECT ROUND(AVG(units), 2) AS avg_units FROM store_sales WHERE units IS NOT NULL OR revenue > 1500 ORDER BY avg_units DESC
