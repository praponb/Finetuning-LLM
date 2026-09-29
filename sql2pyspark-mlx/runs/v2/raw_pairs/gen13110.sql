select
    o.status,
    month(o.order_date) as order_month,
    max(p.paid_at) as last_paid_at,
    round(avg(p.amount), 2) as avg_amount
from payments p
inner join orders o on p.order_id = o.id
inner join stores s on o.store_id = s.id
where p.status = 'REFUNDED'
group by o.status, month(o.order_date);
