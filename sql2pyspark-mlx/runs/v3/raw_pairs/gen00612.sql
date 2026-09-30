SELECT status, MIN(shipping_fee) AS min_shipping_fee
FROM orders
WHERE channel IN ('mobile', 'store', 'partner') AND MONTH(order_date) IN (4, 7, 9)
GROUP BY status
ORDER BY status;
