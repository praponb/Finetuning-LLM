select
    o.id,
    o.amount,
    s.region,
    s.store_name
from orders o
join stores s on o.store_id = s.id
where o.customer_id is not null and s.opened_date < '2023-10-01'
order by store_name desc, id
limit 20
