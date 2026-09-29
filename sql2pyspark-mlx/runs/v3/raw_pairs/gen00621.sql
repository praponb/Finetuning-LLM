select * from inventory where year(last_restock) = 2023 and month(last_restock) in (5, 8, 9) and quantity <= 150 order by quantity, id limit 3;
