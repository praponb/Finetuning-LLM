SELECT status, MAX(shipping_fee) max_shipping_fee
FROM orders
GROUP BY 1
HAVING MAX(shipping_fee) < 24
