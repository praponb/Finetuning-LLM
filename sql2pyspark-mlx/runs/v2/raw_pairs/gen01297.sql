SELECT channel, MIN(order_date) min_order_date, MAX(amount) max_amount, MAX(shipping_fee) - MIN(shipping_fee) shipping_fee_range FROM orders WHERE amount <= 1240 GROUP BY channel ORDER BY channel
