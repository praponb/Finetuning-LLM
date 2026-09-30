select * from (select product_id, order_id, round(stddev(quantity), 2) as stddev_quantity from order_items where order_id in (18, 20) group by product_id, order_id) as t where stddev_quantity >= 56;
