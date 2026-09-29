select o.channel, o.order_date, o.store_id, s.order_id, s.status, o.amount + o.shipping_fee amount_with_shipping from orders o left join shipments s on s.order_id = o.id where s.order_id is not null
