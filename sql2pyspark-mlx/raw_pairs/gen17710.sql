select
    o.id,
    o.amount,
    o.channel,
    o.shipping_fee,
    s.city
from orders o
inner join stores s on o.store_id = s.id
where s.city in ('Chiang Mai', 'Bangkok')
