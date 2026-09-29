select oi.id, oi.product_id, o.amount, o.shipping_fee from order_items oi inner join orders o on oi.order_id = o.id
