select id, store_id, coalesce(quantity, 0) as quantity_filled from inventory;
