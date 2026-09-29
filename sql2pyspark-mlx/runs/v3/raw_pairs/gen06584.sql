select p.status, p.method, o.amount from payments p inner join orders o on p.order_id = o.id order by o.amount desc;
