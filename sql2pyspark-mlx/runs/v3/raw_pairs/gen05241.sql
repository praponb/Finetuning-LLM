select id, last_restock from inventory where last_restock = (select max(last_restock) from inventory) order by last_restock desc, id limit 5
