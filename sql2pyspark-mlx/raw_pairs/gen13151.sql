SELECT s.region, s.city, i.last_restock, ADD_MONTHS(i.last_restock, 3) AS last_restock_plus_months FROM stores s INNER JOIN inventory i ON s.id = i.store_id
