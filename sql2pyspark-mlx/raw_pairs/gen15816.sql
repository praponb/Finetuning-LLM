select p.id, p.amount, o.channel, o.status, o.shipping_fee, round(o.shipping_fee, 1) as shipping_fee_rounded from payments p left outer join orders o on p.order_id = o.id;
