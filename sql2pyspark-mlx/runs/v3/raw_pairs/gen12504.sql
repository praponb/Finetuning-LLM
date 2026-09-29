select id, last_restock, store_id, quantity, date_add(last_restock, 31) as last_restock_plus_31d from inventory order by id desc;
