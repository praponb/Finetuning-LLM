SELECT o.id, o.channel FROM orders o WHERE o.store_id NOT IN (SELECT id FROM stores WHERE region IN ('East', 'South'));
