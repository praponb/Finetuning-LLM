select oi.product_id, oi.quantity, oi.order_id, p.stock_qty from order_items oi inner join products p on oi.product_id = p.id where oi.order_id in (12, 17) and oi.product_id is not null
