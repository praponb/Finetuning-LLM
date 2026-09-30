select
    s.id,
    s.status,
    s.order_id,
    s.cost,
    o.amount,
    o.channel,
    o.discount
from shipments s
inner join orders o on s.order_id = o.id
where month(s.shipped_date) in (3, 6, 11);
