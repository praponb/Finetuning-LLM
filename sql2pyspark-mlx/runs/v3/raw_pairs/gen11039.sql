select order_id, sum(unit_price) as total_unit_price from order_items group by order_id;
