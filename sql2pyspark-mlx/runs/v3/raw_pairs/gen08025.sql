select o.id, count(oi.id) as order_item_count from orders o join order_items oi on oi.order_id = o.id group by o.id order by o.id desc, order_item_count;
