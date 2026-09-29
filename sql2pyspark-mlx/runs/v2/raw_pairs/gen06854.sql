SELECT o.id, o.order_date, s.sqft, s.city, s.opened_date FROM orders o JOIN stores s ON o.store_id = s.id WHERE s.city NOT IN ('London', 'Chiang Mai') ORDER BY opened_date;
