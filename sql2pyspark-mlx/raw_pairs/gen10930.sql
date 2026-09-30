select id, store_id, last_restock, product_id from inventory where product_id in (1, 4, 6) or product_id = 5 order by store_id;
