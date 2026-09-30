SELECT product_id, avg_revenue FROM (SELECT product_id, ROUND(AVG(revenue), 2) AS avg_revenue FROM store_sales GROUP BY product_id) AS s WHERE avg_revenue >= 74;
