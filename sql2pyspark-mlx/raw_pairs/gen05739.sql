SELECT status, ROUND(STDDEV(shipping_fee), 2) stddev_shipping_fee FROM orders GROUP BY status ORDER BY status DESC;
