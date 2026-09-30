select store_id, avg(quantity) as avg_quantity, round(stddev(quantity), 2) as stddev_quantity from inventory where product_id = 7 group by store_id having round(stddev(quantity), 2) < 76;
