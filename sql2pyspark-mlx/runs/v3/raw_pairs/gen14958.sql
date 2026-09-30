SELECT id, channel, order_date, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date, id) AS row_num FROM orders
