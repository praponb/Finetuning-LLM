select p.status, o.discount from payments p right join orders o on p.order_id = o.id order by discount
