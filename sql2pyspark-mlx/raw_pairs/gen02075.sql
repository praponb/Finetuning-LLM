select s.city, avg(p.amount) as avg_amount, min(p.amount) as min_amount
from payments p
left join orders o on p.order_id = o.id
left join stores s on o.store_id = s.id
group by s.city;
