WITH ranked_order_items AS (SELECT *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS row_num FROM order_items) SELECT * FROM ranked_order_items WHERE row_num <= 1;
