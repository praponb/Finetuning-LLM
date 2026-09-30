SELECT channel, status, ROUND(STDDEV(shipping_fee), 2) AS stddev_shipping_fee, MIN(shipping_fee) AS min_shipping_fee FROM orders GROUP BY channel, status ORDER BY status DESC;
