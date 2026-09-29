select s.city, min(i.quantity) as min_quantity, max(i.last_restock) as latest_last_restock
from inventory i
inner join stores s on i.store_id = s.id
where i.store_id in (1, 3, 6)
group by s.city
