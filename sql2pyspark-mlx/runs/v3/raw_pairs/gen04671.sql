SELECT id, store_id, amount, MIN(amount) OVER (PARTITION BY store_id) AS store_id_min_amount FROM orders WHERE shipping_fee BETWEEN 20 AND 24 ORDER BY amount;
