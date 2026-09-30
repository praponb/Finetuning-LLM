select last_restock from inventory where product_id in (3, 5, 6, 10) except select last_restock from inventory where last_restock > '2024-04-19'
