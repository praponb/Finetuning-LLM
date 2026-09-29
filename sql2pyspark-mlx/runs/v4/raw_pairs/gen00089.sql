select
    s.id,
    s.status,
    o.customer_id,
    o.discount,
    o.amount
from shipments s
inner join orders o on o.id = s.order_id
where s.carrier = 'FedEx'
