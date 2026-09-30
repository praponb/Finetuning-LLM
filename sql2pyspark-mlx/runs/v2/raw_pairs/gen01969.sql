select store_id, round(quantity, 1) as quantity_rounded, ceil(quantity) as quantity_whole from inventory;
