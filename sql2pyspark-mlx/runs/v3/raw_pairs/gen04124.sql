SELECT o.id, o.status, o.channel FROM orders o WHERE o.store_id NOT IN (SELECT id FROM stores WHERE sqft BETWEEN 7230 AND 8580) ORDER BY channel
