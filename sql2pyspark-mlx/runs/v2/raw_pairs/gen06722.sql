SELECT id, channel, order_date FROM (SELECT id, channel, order_date, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date DESC, id) row_num FROM orders) t WHERE row_num = 1;
