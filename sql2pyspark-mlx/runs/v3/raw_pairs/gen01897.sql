select
    p.order_id,
    o.customer_id,
    s.city,
    s.region
from payments p
inner join orders o on p.order_id = o.id
left join stores s on o.store_id = s.id
where s.city = 'Tokyo';
