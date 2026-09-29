select order_id, sum(unit_price) as total_unit_price, sum(sum(unit_price)) over (order by order_id) as cumulative_unit_price from order_items group by order_id order by total_unit_price desc
