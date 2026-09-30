select p.id, p.order_id, p.paid_at, o.status, datediff(current_date(), p.paid_at) days_since_paid_at from payments p inner join orders o on o.id = p.order_id order by p.id desc;
