select store_id, min(quantity) as min_quantity, max(quantity) - min(quantity) as quantity_range from inventory group by store_id having max(quantity) < 170;
