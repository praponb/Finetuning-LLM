SELECT s.region, s.opened_date, i.last_restock FROM stores s INNER JOIN inventory i ON i.store_id = s.id
