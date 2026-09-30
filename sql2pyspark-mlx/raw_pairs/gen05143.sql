select o.id, o.channel, count(oi.id) order_item_count, count(oi.quantity) quantity_count from orders o left join order_items oi on oi.order_id = o.id group by o.id, o.channel order by 2;
