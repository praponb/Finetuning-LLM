SELECT id, channel, shipping_fee, DENSE_RANK() OVER (PARTITION BY channel ORDER BY shipping_fee) AS dense_rnk FROM orders WHERE store_id IS NOT NULL;
