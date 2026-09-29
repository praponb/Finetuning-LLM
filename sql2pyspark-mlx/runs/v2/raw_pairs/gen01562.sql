select
    s.status,
    o.channel,
    c.name,
    c.city,
    concat(s.status, '/', s.carrier) as status_carrier
from shipments s
join orders o on s.order_id = o.id
join customers c on o.customer_id = c.id
order by c.name
