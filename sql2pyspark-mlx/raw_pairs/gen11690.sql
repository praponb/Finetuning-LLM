SELECT id, channel, store_id FROM orders WHERE discount <= 0.35 AND amount BETWEEN 500 AND 1150 ORDER BY id LIMIT 50
