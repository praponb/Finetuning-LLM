select
    i.id,
    i.quantity,
    p.price,
    p.launch_date,
    s.store_name,
    s.opened_date
from inventory i
left join products p on i.product_id = p.id
left join stores s on i.store_id = s.id
where s.region is not null
order by p.price desc;
