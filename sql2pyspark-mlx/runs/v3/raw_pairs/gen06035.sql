select id, order_id, unit_price, quantity, lead(quantity, 1) over (partition by order_id order by unit_price, id) as next_quantity from order_items order by next_quantity desc;
