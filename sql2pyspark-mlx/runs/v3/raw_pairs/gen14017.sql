select s.region, round(avg(oi.quantity), 2) avg_quantity
from order_items oi
inner join orders o on oi.order_id = o.id
inner join stores s on o.store_id = s.id
group by s.region
order by avg_quantity asc;
