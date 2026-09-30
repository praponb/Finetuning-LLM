select store_id, max(quantity) as max_quantity, round(stddev(quantity), 2) as stddev_quantity from inventory group by 1 order by max_quantity desc;
