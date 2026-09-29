select product_id, coalesce(quantity, 0) quantity_filled, cast(quantity as double) quantity_str from inventory where quantity > 80 and product_id in (1, 4, 8);
