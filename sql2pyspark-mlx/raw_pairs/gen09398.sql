select count(distinct quantity) as unique_stores, avg(quantity) as avg_quantity, min(quantity) as min_quantity from inventory;
