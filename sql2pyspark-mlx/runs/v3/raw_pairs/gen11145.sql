SELECT o.id, o.channel FROM orders o WHERE o.store_id NOT IN (SELECT id FROM stores WHERE store_name LIKE 'Sia%') ORDER BY 2 DESC, id
