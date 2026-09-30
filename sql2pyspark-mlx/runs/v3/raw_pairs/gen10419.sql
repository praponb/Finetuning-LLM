select o.status, p.amount, p.order_id from orders o left join payments p on p.order_id = o.id where hour(p.paid_at) between 7 and 12;
