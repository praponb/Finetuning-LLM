select id, order_id, quantity, unit_price, lead(unit_price, 1) over (partition by order_id order by quantity, id) as next_unit_price from order_items
