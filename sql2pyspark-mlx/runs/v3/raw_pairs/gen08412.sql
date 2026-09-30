SELECT s.sqft, s.city, o.amount, o.store_id, o.shipping_fee FROM stores s INNER JOIN orders o ON s.id = o.store_id ORDER BY sqft DESC;
