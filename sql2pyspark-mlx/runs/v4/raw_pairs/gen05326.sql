select id, last_restock from inventory where last_restock >= (select max(last_restock) from inventory);
