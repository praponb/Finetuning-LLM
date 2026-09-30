select order_id, sum(unit_price * quantity) as revenue from order_items where order_id in (18, 28, 30, 33) group by order_id order by revenue, order_id;
