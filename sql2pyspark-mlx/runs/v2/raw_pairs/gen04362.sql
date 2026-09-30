select s.region, count(i.id) as inventory_count
from stores s
inner join inventory i on s.id = i.store_id
where i.store_id in (3, 5)
group by s.region
order by s.region desc, inventory_count;
