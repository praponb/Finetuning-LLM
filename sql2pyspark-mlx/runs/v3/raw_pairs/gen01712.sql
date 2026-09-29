SELECT id, channel, customer_id FROM orders WHERE (customer_id IS NOT NULL AND discount >= 0.27) OR status = 'CANCELLED' ORDER BY id LIMIT 5;
