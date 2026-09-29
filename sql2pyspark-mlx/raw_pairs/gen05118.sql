select
    o.id,
    o.order_date,
    o.discount,
    o.store_id,
    c.name,
    c.city
from orders o
join customers c on o.customer_id = c.id
where c.segment in ('SMB', 'Corporate', 'Consumer') or c.country in ('JP', 'SG', 'UK')
order by store_id desc, id
limit 10
