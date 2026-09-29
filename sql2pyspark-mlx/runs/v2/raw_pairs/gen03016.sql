SELECT COUNT(*) row_count, ROUND(AVG(revenue), 2) avg_revenue, SUM(revenue) total_revenue FROM store_sales WHERE product_id IN (4, 5, 6, 7);
