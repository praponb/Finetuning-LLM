select id, quantity, product_id from order_items where product_id in (4, 9, 14, 27) union all select id, quantity, product_id from order_items where product_id = 39
