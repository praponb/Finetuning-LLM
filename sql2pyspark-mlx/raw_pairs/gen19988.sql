select product_id, store_id, coalesce(quantity, 0) as quantity_filled from inventory where product_id = 8 order by quantity_filled desc, product_id desc;
