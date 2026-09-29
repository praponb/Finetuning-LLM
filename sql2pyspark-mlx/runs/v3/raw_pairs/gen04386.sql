select quantity, order_id, unit_price, least(unit_price, quantity) as least_unit_price_quantity from order_items
