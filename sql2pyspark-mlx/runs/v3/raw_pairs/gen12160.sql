select
    o.status,
    o.amount,
    o.channel,
    s.city,
    s.sqft,
    s.opened_date
from orders o
left outer join stores s on o.store_id = s.id
where s.sqft != 12908
