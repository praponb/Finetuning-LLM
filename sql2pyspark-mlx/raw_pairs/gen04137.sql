select
    o.id,
    o.channel,
    o.discount,
    s.city,
    s.opened_date
from orders o
left outer join stores s on o.store_id = s.id
where o.store_id in (33, 38);
