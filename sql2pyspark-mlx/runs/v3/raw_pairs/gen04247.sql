SELECT store_id, avg_revenue FROM (SELECT store_id, AVG(revenue) AS avg_revenue FROM store_sales WHERE product_id = 10 GROUP BY store_id) AS s WHERE avg_revenue >= 3400 ORDER BY store_id DESC;
