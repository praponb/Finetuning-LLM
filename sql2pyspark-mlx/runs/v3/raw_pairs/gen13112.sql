select
    o.id,
    o.channel,
    c.email,
    c.is_active,
    s.region,
    s.opened_date,
    ceil(c.credit_limit) as credit_limit_whole
from orders o
inner join customers c on c.id = o.customer_id
inner join stores s on o.store_id = s.id
where s.region in ('West', 'North')
order by 3 desc, id
limit 5;
