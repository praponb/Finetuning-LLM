SELECT store_id, channel FROM orders WHERE status = 'CANCELLED' UNION ALL SELECT store_id, channel FROM orders WHERE channel IN ('partner', 'store', 'web') ORDER BY channel;
