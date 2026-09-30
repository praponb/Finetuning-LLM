select
    s.id,
    s.store_name,
    count(i.id) as inventory_count,
    round(stddev(i.quantity), 2) as stddev_quantity,
    count(i.quantity) as quantity_count
from stores s
left join inventory i on s.id = i.store_id
group by s.id, s.store_name
order by s.store_name;
