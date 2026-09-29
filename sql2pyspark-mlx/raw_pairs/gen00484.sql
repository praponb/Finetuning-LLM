select id, last_restock, date_trunc('MONTH', last_restock) as last_restock_period from inventory where product_id = 4 or year(last_restock) = 2023 order by id, last_restock
