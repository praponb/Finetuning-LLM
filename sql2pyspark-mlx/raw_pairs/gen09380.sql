select order_id, quantity from order_items where product_id is null union select order_id, quantity from order_items where quantity < 7;
