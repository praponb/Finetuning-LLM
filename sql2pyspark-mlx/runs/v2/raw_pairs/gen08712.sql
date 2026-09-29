select id, unit_price, lag(unit_price, 1) over (order by unit_price, id) as prev_unit_price from order_items where quantity != 7;
