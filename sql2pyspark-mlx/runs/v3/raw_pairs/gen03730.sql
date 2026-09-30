select date_format(o.order_date, 'yyyy-MM') as order_month, count(oi.quantity) as quantity_count
from order_items oi
join orders o on o.id = oi.order_id
left outer join products p on oi.product_id = p.id
group by date_format(o.order_date, 'yyyy-MM')
having count(oi.quantity) >= 4
order by quantity_count desc, order_month
limit 1;
