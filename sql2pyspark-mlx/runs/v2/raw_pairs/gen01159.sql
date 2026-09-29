SELECT s.id, s.opened_date, s.sqft, s.region FROM stores s WHERE s.city IN ('Tokyo', 'New York', 'London') AND s.id IN (SELECT store_id FROM orders WHERE shipping_fee > 14)
