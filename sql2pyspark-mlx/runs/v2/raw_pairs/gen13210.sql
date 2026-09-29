select p.id, p.amount, p.paid_at, o.discount, o.store_id, o.amount as order_amount from payments p inner join orders o on p.order_id = o.id order by p.id;
