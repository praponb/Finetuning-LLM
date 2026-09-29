select o.status, round(avg(oi.quantity), 2) avg_quantity, min(oi.unit_price) min_unit_price
from order_items oi
left join orders o on o.id = oi.order_id
left join customers c on o.customer_id = c.id
where c.city in ('Tokyo', 'Bangkok')
group by o.status
order by status desc;
