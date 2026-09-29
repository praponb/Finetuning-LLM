select id, last_restock, quantity, round(quantity, 2) as quantity_rounded from inventory where last_restock > '2025-05-16';
