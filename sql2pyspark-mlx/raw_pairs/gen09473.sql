select id, last_restock, store_id, dayofweek(last_restock) as last_restock_dayofweek from inventory where product_id in (1, 9)
