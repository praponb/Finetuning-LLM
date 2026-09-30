select
    oi.id,
    oi.order_id,
    o.order_date,
    s.opened_date,
    s.region
from order_items oi
inner join orders o on oi.order_id = o.id
inner join stores s on o.store_id = s.id
where s.opened_date <= '2023-10-27' or oi.product_id in (32, 40)
order by s.region, id
limit 50
