select oi.id, oi.quantity, oi.order_id, o.discount, o.channel, p.launch_date from order_items oi right outer join orders o on o.id = oi.order_id left outer join products p on oi.product_id = p.id;
