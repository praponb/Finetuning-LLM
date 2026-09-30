SELECT o.id, o.channel, o.discount, s.city, s.opened_date, s.region FROM orders o LEFT JOIN stores s ON s.id = o.store_id WHERE o.customer_id IN (18, 24, 34, 38)
