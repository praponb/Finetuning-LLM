select
    s.id,
    s.store_name,
    count(i.id) as inventory_count,
    max(i.quantity) - min(i.quantity) as quantity_range,
    count(distinct i.quantity) as unique_stores
from stores s
join inventory i on s.id = i.store_id
where s.city = 'Tokyo'
group by s.id, s.store_name
order by s.store_name;
