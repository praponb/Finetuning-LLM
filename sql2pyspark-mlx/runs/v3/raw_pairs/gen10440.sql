select oi.id, oi.quantity, oi.order_id, p.launch_date, p.supplier_id, p.price from order_items oi inner join products p on oi.product_id = p.id where oi.quantity != 5 order by id desc limit 3;
