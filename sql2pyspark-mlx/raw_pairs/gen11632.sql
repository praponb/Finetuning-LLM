select
    dim.region,
    e.status,
    count(*) as rows_total,
    min(e.discount) as min_discount,
    sum(e.shipping_fee) as total_shipping_fee
from orders e
join stores dim on e.store_id = dim.id
group by dim.region, e.status
order by rows_total desc nulls first;
