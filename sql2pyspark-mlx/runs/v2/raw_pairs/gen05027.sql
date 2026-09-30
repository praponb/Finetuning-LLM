select
    o.id,
    o.discount,
    o.customer_id,
    s.sqft,
    c.segment,
    c.name
from orders o
left outer join stores s on s.id = o.store_id
left outer join customers c on o.customer_id = c.id
where s.sqft < 11381
order by name, id
limit 10;
