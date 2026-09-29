select p.id, p.paid_at, p.amount, p.order_id, o.discount, o.channel from payments p left join orders o on p.order_id = o.id order by o.discount desc
