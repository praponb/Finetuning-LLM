select orders.channel, sum(order_items.unit_price * order_items.quantity) as revenue, sum(order_items.unit_price) as total_unit_price
from order_items
left join orders on order_items.order_id = orders.id
where orders.channel in ('partner', 'mobile', 'web')
group by orders.channel
order by total_unit_price, channel
limit 5;
