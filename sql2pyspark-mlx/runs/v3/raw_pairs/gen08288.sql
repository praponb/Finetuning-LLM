select id, order_id, quantity, unit_price, quantity + unit_price as line_total, quantity - unit_price as line_total_2 from order_items where order_id in (16, 19, 28);
