select o.channel, min(oi.unit_price) min_unit_price
from order_items oi
inner join orders o on oi.order_id = o.id
group by o.channel
order by o.channel
limit 1;
