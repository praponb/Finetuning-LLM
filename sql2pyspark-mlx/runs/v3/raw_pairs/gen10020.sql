select id, store_id, last_restock, product_id from inventory where not (product_id in (2, 3, 7, 10) and last_restock between '2024-01-05' and '2024-10-01');
