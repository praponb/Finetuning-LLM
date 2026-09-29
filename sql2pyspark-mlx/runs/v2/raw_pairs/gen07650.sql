SELECT product_id, SUM(units) AS total_units, SUM(revenue) AS total_revenue FROM store_sales GROUP BY 1 ORDER BY total_revenue;
