select
    date_trunc('MONTH', orders.order_date) as order_month,
    orders.channel,
    min(shipments.cost) as min_cost,
    avg(shipments.cost) as avg_cost
from shipments
inner join orders on shipments.order_id = orders.id
group by date_trunc('MONTH', orders.order_date), orders.channel
having avg_cost >= 66;
