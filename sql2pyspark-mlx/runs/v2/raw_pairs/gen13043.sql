select o.id, o.channel, o.shipping_fee, o.order_date from orders o where exists (select 1 from payments p where p.order_id = o.id and to_date(p.paid_at) < '2024-10-01')
