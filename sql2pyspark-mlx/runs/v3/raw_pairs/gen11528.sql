SELECT id, customer_id, shipping_fee, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee) AS shipping_fee_dense_rank FROM orders WHERE discount >= 0.12;
