select id, last_restock, product_id from inventory where last_restock = (select min(last_restock) from inventory where last_restock between '2023-12-01' and '2024-02-01') order by id desc limit 5
