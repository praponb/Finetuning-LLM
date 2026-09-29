SELECT id, channel, amount, order_date, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC, id) AS amount_row, ROW_NUMBER() OVER (ORDER BY order_date DESC, id) AS row_num FROM orders;
