SELECT id, store_id, product_id, revenue, MAX(revenue) OVER (PARTITION BY store_id, product_id) AS store_id_max_revenue FROM store_sales;
