select o.status, max(oi.unit_price) - min(oi.unit_price) as unit_price_range, avg(oi.unit_price) as avg_unit_price
from order_items oi
right outer join orders o on o.id = oi.order_id
where o.channel = 'partner'
group by o.status
order by unit_price_range, status;
