SELECT id, store_id, shipping_fee, channel FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY shipping_fee DESC) AS pos FROM orders) AS t WHERE pos <= 2 ORDER BY shipping_fee;
