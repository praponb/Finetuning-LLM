SELECT s.opened_date, i.product_id, i.last_restock, i.store_id FROM stores s LEFT JOIN inventory i ON s.id = i.store_id
