select
    s.region,
    o.channel,
    o.amount,
    o.shipping_fee
from stores s
left join orders o on s.id = o.store_id
where s.store_name like '%Old%' and s.sqft < 10347
order by region desc;
