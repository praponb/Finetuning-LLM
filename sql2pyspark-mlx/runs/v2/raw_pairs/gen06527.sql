select id, last_restock, quantity, coalesce(quantity, 0) as quantity_filled, round(quantity, 0) as quantity_rounded from inventory order by quantity_filled, id limit 5;
