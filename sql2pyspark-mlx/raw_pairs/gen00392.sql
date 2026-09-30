select
    s.city,
    count(*) as rows_total,
    sum(a.quantity) as total_quantity
from inventory a
left join stores s on a.store_id = s.id
group by s.city;
