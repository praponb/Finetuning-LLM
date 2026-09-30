select id, last_restock, product_id from inventory where last_restock = (select min(last_restock) from inventory where quantity between 113 and 134)
