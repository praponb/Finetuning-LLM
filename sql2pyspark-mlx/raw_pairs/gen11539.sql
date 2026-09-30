SELECT channel, COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count FROM orders WHERE shipping_fee < 23 GROUP BY channel
