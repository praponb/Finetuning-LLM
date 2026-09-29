select p.method, p.paid_at, o.channel, o.shipping_fee, o.amount from payments p right outer join orders o on p.order_id = o.id;
