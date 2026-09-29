select o.id, o.shipping_fee, t.max_amount from orders o left join (select order_id, max(amount) as max_amount from payments group by order_id) t on o.id = t.order_id where t.max_amount >= 940;
