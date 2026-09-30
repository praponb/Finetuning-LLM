select month(o.order_date) order_month, count(s.cost) cost_count, sum(s.cost) total_cost
from shipments s
left join orders o on o.id = s.order_id
where s.carrier not in ('Kerry', 'DHL')
group by month(o.order_date)
having sum(s.cost) > 250
order by total_cost desc, order_month desc
limit 5;
