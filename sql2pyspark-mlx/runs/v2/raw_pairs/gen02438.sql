SELECT id, channel, shipping_fee FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY channel ORDER BY shipping_fee DESC) AS pos FROM orders) AS t WHERE pos = 1;
