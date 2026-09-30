select id, store_id, last_restock from inventory where product_id = 6 except select id, store_id, last_restock from inventory where product_id in (4, 5, 6, 10);
