select p.paid_at, p.amount, o.amount as order_amount, o.store_id, o.channel, o.amount * o.discount as amount_times_discount from payments p right join orders o on p.order_id = o.id order by p.amount;
