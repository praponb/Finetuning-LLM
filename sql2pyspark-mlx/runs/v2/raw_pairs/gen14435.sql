select o.id, o.status, o.shipping_fee, t.total_quantity from orders o left join (select order_id, sum(quantity) as total_quantity from order_items group by order_id) t on o.id = t.order_id
