SELECT id, store_id, shipping_fee FROM (SELECT id, store_id, shipping_fee, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee DESC, id) AS pos FROM orders) AS t WHERE pos <= 5;
