select o.store_id, round(avg(p.amount), 2) as avg_amount, min(p.paid_at) as first_paid_at from payments p join orders o on o.id = p.order_id group by o.store_id order by avg_amount
