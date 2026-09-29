select date_format(o.order_date, 'yyyy-MM') as order_month, count(s.cost) as cost_count
from shipments s
join orders o on s.order_id = o.id
full outer join customers c on o.customer_id = c.id
group by date_format(o.order_date, 'yyyy-MM')
order by order_month;
