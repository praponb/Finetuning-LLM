SELECT MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range, COUNT(*) AS order_count, ROUND(STDDEV(discount), 2) AS stddev_discount
FROM orders
WHERE amount <= 1020
ORDER BY shipping_fee_range DESC;
