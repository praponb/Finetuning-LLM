SELECT id, status, shipping_fee, DENSE_RANK() OVER (PARTITION BY status ORDER BY shipping_fee DESC) AS shipping_fee_dense_rank FROM orders;
