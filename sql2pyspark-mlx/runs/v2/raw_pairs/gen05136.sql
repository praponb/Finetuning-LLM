SELECT DATE_TRUNC('MONTH', sale_date) AS sale_month, MIN(revenue) AS min_revenue, MAX(units) AS max_units FROM store_sales WHERE units <= 27 GROUP BY 1 ORDER BY sale_month, min_revenue;
