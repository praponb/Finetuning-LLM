select
    o.id,
    o.channel,
    o.shipping_fee,
    s.region,
    s.store_name,
    s.sqft
from orders o
inner join stores s on o.store_id = s.id
order by s.store_name;
