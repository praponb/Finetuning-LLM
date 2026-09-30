SELECT id, store_id, order_date, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date DESC, id) AS row_num FROM orders WHERE MONTH(order_date) IN (1, 4, 11)
