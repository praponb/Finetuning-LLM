select s.region, count(o.id) as order_count
from stores s
left join orders o on o.store_id = s.id
where o.channel = 'store'
group by s.region
having count(o.id) > 5
order by region desc;
