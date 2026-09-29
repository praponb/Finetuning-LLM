select oi.id, oi.quantity, oi.order_id, p.product_name, p.cost, p.price from order_items oi left join products p on oi.product_id = p.id where oi.product_id is null order by product_name;
