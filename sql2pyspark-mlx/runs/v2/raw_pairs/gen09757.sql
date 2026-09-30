select id, store_id, quantity, day(last_restock) as last_restock_day from inventory where quantity <= 146 and product_id in (2, 8) and year(last_restock) = 2024;
