SELECT id, channel, shipping_fee, order_date FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY channel ORDER BY shipping_fee DESC) AS pos FROM orders) AS ranked WHERE pos = 1 ORDER BY shipping_fee DESC
