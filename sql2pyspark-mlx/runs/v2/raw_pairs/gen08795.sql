select product_id from order_items where order_id in (10, 28, 34) intersect select product_id from order_items where order_id in (24, 25) order by product_id
