SELECT AVG(units) avg_units, MIN(sale_date) first_sale_date, SUM(revenue) total_revenue
FROM store_sales
WHERE product_id = 8 AND sale_date <= '2025-07-22'
ORDER BY avg_units DESC
