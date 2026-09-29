select id, order_id, unit_price, quantity + unit_price as line_total, unit_price + quantity as line_total_2 from order_items where quantity > 4
