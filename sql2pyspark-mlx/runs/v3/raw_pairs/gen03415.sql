select
    orders.status,
    orders.channel,
    sum(case when shipments.carrier = 'FedEx' then shipments.cost else 0 end) as fedex_cost,
    count(*) as shipment_count
from shipments
inner join orders on shipments.order_id = orders.id
where orders.order_date < '2023-03-14'
group by orders.status, orders.channel
order by shipment_count desc, status, channel
limit 50;
