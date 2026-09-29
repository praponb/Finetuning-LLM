SELECT id, amount, order_date, shipping_fee, ROW_NUMBER() OVER (ORDER BY amount DESC, id) AS row_num, LAG(shipping_fee, 2) OVER (ORDER BY order_date, id) AS prev_shipping_fee FROM orders
