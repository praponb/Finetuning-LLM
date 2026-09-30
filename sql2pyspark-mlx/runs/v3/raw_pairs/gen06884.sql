select id, quantity, unit_price, ceil(unit_price) as unit_price_whole from order_items where quantity = 3
