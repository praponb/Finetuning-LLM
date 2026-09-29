select s.id, s.store_name, count(i.id) as inventory_count
from stores s
left outer join inventory i on s.id = i.store_id
group by s.id, s.store_name
having inventory_count > 4
order by s.store_name, id
limit 10
