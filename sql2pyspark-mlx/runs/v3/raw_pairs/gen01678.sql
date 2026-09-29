select id, product_id, quantity, unit_price, lead(unit_price, 1) over (partition by product_id order by quantity, id) as next_unit_price from order_items where order_id in (27, 34);
