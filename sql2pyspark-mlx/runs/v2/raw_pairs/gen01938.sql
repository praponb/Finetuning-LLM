SELECT s.region, s.opened_date, i.product_id FROM stores s LEFT OUTER JOIN inventory i ON s.id = i.store_id WHERE i.product_id IN (1, 5) AND s.sqft > 10374
