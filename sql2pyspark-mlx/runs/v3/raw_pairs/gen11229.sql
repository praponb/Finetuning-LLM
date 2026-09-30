select i.product_id, s.city, s.region
from inventory i
left outer join stores s on i.store_id = s.id
where i.product_id = 2
order by s.city asc;
