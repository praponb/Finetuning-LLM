SELECT id, status, shipping_fee, DENSE_RANK() OVER (PARTITION BY status ORDER BY shipping_fee) AS dense_rnk FROM orders;
