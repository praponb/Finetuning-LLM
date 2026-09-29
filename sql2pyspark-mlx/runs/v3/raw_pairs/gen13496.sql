SELECT channel, MIN(order_date) AS min_order_date FROM orders WHERE shipping_fee IS NOT NULL AND store_id = 6 GROUP BY channel;
