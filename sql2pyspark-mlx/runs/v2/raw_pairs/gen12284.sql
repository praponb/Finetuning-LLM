SELECT product_id, ROUND(AVG(revenue), 2) AS avg_revenue FROM store_sales WHERE (units < 39 AND units >= 25) OR product_id IN (1, 3) GROUP BY product_id;
