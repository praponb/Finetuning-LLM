SELECT * FROM (SELECT product_id, ROUND(AVG(revenue), 2) avg_revenue FROM store_sales WHERE sale_date < '2025-01-01' GROUP BY product_id) agg WHERE avg_revenue >= 51 ORDER BY product_id ASC;
