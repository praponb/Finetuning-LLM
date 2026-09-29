select o.id, o.customer_id, o.channel, o.shipping_fee from orders o where exists (select 1 from payments p where p.order_id = o.id and p.paid_at < '2023-12-01') order by o.channel desc;
