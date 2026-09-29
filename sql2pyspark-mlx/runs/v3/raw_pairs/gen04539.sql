SELECT id, status, shipping_fee, store_id FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY shipping_fee DESC) AS rnk FROM orders) AS r WHERE rnk <= 3;
