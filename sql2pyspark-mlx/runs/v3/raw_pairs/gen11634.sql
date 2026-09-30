select o.id, o.shipping_fee, o.customer_id, o.channel, c.is_active from orders o inner join customers c on o.customer_id = c.id order by o.id desc;
