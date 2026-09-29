select s.region, max(i.last_restock) as latest_last_restock, avg(i.quantity) as avg_quantity
from inventory i
right outer join stores s on i.store_id = s.id
where i.last_restock between '2024-03-05' and '2025-09-01'
group by s.region
having avg(i.quantity) > 140
order by s.region;
