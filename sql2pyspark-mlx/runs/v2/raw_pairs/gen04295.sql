select id, unit_price, order_id, last_value(order_id) over (order by unit_price desc, id rows between unbounded preceding and unbounded following) last_order_id from order_items
