select o.channel, count(*) as num_payments, min(p.paid_at) as first_paid_at from payments p inner join orders o on p.order_id = o.id where p.method is not null group by o.channel
