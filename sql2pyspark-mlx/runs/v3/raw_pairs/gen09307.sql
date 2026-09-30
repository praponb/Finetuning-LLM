select
    s.store_name,
    s.region,
    s.city,
    o.status,
    o.store_id,
    o.channel
from stores s
inner join orders o on s.id = o.store_id
where s.sqft < 14388
order by s.store_name;
