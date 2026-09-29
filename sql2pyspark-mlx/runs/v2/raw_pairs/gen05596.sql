SELECT i.id, i.last_restock, s.region, p.price FROM inventory i LEFT OUTER JOIN stores s ON i.store_id = s.id INNER JOIN products p ON i.product_id = p.id ORDER BY i.last_restock
