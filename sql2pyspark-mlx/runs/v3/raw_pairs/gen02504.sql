select p.id, p.amount, o.discount, c.segment from payments p join orders o on p.order_id = o.id join customers c on c.id = o.customer_id order by p.id desc
