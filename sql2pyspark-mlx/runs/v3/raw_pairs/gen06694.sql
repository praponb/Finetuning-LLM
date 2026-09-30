SELECT * FROM (SELECT id, store_id, shipping_fee, status, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY shipping_fee) AS rn FROM orders) AS t WHERE rn <= 1 ORDER BY store_id;
