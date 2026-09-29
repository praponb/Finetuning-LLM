select *, round(quantity, 1) as quantity_rounded from inventory where store_id in (3, 4, 5, 6);
