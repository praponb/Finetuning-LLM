select orders.id, count(order_items.id) as order_item_count from orders left outer join order_items on orders.id = order_items.order_id group by orders.id order by 1;
