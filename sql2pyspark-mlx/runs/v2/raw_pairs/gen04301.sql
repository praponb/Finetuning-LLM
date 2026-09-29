select store_id, min(last_restock) as first_last_restock, min(quantity) as min_quantity from inventory group by store_id having min_quantity >= 210 order by store_id desc;
