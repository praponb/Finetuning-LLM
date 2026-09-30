select s.region, s.city, count(case when o.status = 'SHIPPED' then 1 end) shipped_count
from orders o
join stores s on o.store_id = s.id
group by s.region, s.city
