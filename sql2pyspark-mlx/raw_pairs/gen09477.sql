select id, quantity, quantity * unit_price as line_total from order_items where product_id = 4 and product_id is not null order by id desc;
