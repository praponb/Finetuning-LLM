SELECT store_id, ROUND(STDDEV(shipping_fee), 2) stddev_shipping_fee
FROM orders
WHERE shipping_fee < 39 AND (shipping_fee >= 38 OR store_id = 19)
GROUP BY store_id;
