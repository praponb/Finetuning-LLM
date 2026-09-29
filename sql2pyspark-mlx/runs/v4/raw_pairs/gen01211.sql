SELECT s.region, o.amount, o.status FROM stores s LEFT JOIN orders o ON s.id = o.store_id
