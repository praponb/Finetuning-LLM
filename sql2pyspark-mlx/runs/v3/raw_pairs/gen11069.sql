select store_id, count(distinct quantity) as unique_products, max(quantity) - min(quantity) as quantity_range from inventory group by store_id;
