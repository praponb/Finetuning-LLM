SELECT id, store_id, revenue, AVG(revenue) OVER (PARTITION BY store_id) AS store_id_avg_revenue FROM store_sales;
