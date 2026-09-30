select id, quantity from inventory where last_restock >= '2024-06-30' and store_id in (1, 6) order by quantity desc, id
