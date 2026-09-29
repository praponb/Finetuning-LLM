SELECT YEAR(order_date) AS order_year, MIN(shipping_fee) AS min_shipping_fee
FROM orders
WHERE store_id IN (3, 8)
GROUP BY YEAR(order_date)
HAVING MIN(shipping_fee) > 28;
