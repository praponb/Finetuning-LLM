SELECT s.sqft, s.region, o.customer_id, o.channel FROM stores s LEFT JOIN orders o ON o.store_id = s.id WHERE o.status = 'SHIPPED'
