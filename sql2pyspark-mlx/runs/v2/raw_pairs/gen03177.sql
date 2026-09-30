select o.status, min(oi.quantity) as min_quantity, round(avg(oi.unit_price), 2) as avg_unit_price
from order_items oi
inner join orders o on oi.order_id = o.id
where oi.unit_price <= 110
group by o.status
having avg_unit_price > 46;
