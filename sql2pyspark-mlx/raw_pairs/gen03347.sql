SELECT order_date FROM orders WHERE channel IN ('store', 'mobile', 'partner') INTERSECT SELECT order_date FROM orders WHERE order_date <= '2024-10-01'
