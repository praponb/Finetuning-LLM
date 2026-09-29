select s.region, sum(p.amount) as total_amount, avg(p.amount) as avg_amount
from payments p
inner join orders o on p.order_id = o.id
inner join stores s on o.store_id = s.id
group by s.region
having avg(p.amount) >= 610;
