SELECT s.store_name, i.last_restock, i.quantity, DATE_ADD(i.last_restock, 80) last_restock_plus_80d FROM stores s INNER JOIN inventory i ON s.id = i.store_id WHERE s.opened_date > '2023-05-01';
