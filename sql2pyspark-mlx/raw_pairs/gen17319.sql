select id, quantity, product_id from order_items where order_id in (22, 27, 33, 35) union select id, quantity, product_id from order_items where order_id = 31 order by id;
