select oi.id, oi.quantity, oi.unit_price, o.store_id, o.customer_id, c.city, c.segment from order_items oi inner join orders o on oi.order_id = o.id left join customers c on o.customer_id = c.id
