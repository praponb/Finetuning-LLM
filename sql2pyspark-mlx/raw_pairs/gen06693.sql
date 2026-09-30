select product_id, order_id from order_items where product_id in (4, 6, 23, 29) union all select product_id, order_id from order_items where order_id in (17, 24, 26, 37) order by product_id desc;
