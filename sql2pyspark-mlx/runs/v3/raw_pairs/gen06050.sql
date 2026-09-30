SELECT i.id, i.last_restock, p.brand, s.region FROM inventory i LEFT OUTER JOIN products p ON i.product_id = p.id INNER JOIN stores s ON i.store_id = s.id
