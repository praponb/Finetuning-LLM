select
    s.id,
    s.shipped_date,
    s.delivered_date,
    o.amount,
    c.email,
    c.is_active
from shipments s
inner join orders o on s.order_id = o.id
inner join customers c on o.customer_id = c.id
where s.cost >= 93;
