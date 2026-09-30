select oi.id, oi.product_id, p.launch_date, p.price, o.status from order_items oi left join products p on p.id = oi.product_id left join orders o on o.id = oi.order_id where o.store_id = 16
