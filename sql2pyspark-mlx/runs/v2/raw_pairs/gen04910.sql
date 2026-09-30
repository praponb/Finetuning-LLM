select store_id, count(quantity) as quantity_count, sum(quantity) as total_quantity from inventory where product_id = 3 and quantity >= 119 group by store_id order by total_quantity;
