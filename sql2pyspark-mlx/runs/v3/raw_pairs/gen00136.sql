select oi.id, oi.product_id, oi.order_id, oi.unit_price from order_items oi where oi.order_id in (select id from orders where channel <> 'store') order by product_id desc, id
