SELECT * FROM (SELECT id, store_id, amount, shipping_fee, RANK() OVER (PARTITION BY store_id ORDER BY amount) AS pos FROM orders WHERE store_id IN (5, 34, 36)) AS t WHERE pos <= 1
