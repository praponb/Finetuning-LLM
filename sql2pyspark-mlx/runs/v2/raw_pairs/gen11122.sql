select store_id, round(stddev(quantity), 2) as stddev_quantity from inventory group by store_id order by stddev_quantity
