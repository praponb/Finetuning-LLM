select round(avg(quantity), 2) as avg_quantity, max(last_restock) as last_last_restock from inventory where store_id in (2, 3, 6) and store_id = 4 and store_id = 3
