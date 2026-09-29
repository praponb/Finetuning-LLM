SELECT *, DATE_ADD(order_date, 11) AS order_date_plus_11d, TRIM(status) AS clean_status FROM orders WHERE channel = 'store' ORDER BY discount, order_date_plus_11d DESC, id LIMIT 1
