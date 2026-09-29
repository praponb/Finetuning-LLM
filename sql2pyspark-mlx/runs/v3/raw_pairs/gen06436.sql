select id, unit_price, quantity, product_id, round(unit_price * 1.1, 2) as adjusted_unit_price from order_items
