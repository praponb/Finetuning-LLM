select order_id, avg(unit_price) as avg_unit_price, round(stddev(unit_price), 2) as stddev_unit_price from order_items where quantity between 3 and 8 group by order_id;
